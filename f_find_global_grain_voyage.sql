
-- Function: dev.zxw_f_find_global_grain_voyage()

-- DROP FUNCTION dev.zxw_f_find_global_grain_voyage();

CREATE OR REPLACE FUNCTION dev.zxw_f_find_global_grain_voyage()
  RETURNS void AS
$BODY$
BEGIN
DROP TABLE IF EXISTS t_cat_prod;
CREATE TEMP TABLE t_cat_prod AS
(SELECT b.cmdty,
       a.cd_report,
       product_code code
FROM cat_product a
LEFT JOIN lookup.cmdty b ON a.cd_report = b.cd_report
UNION

(SELECT 'C'::bpchar cmdty,
        'CRUDE'::bpchar cd_report,
        crude_code code
FROM cat_crude
WHERE crude_code NOT IN
   (SELECT product_code
    FROM cat_product)));

-- build arrival table which contains imo and country and city codes.
DROP TABLE IF EXISTS t_asvt_arrival;


CREATE TEMP TABLE t_asvt_arrival AS
  (SELECT b.imo,
          c.lo_country_code,
          c.lo_city_code,
          a.*
   FROM asvt_arrival a
   LEFT JOIN as_vessel_exp b ON a.vessel = b.vessel
   LEFT JOIN as_poi c ON a.poi = c.poi);


CREATE INDEX indx_imo_poi_date_arrive_asvt_arrival ON t_asvt_arrival (imo, poi, date_arrive);

-- Get grain data from inchcape.iss_process_2.
DROP TABLE IF EXISTS t_grain;

CREATE TEMP TABLE t_grain
AS (
    SELECT
        ((row_number()
                OVER w) + 10000)::int rec_id,
        a.*
    FROM
        inchcape.iss_process_2 a
    WHERE
        grade IN (
            SELECT
                code
            FROM
                t_cat_prod
            WHERE
                cmdty = 'R')
            WINDOW w AS (
            ORDER BY
                vessel,
                operation_date));

-- update quantity values if quantity is less than 100.
UPDATE
    t_grain AS t0
SET
    quantity = t1.revised_quantity
FROM (
    SELECT
        b.dwt, ( CASE WHEN a.quantity < b.dwt / 1000. THEN
                a.quantity * 1000
            ELSE
                b.dwt
END) revised_quantity,
a.*
FROM
    t_grain a
    LEFT JOIN tanker b ON a.imo = b.imo
WHERE
    quantity < 100
    AND quantity > 0
ORDER BY
    quantity) AS t1
WHERE
    t0.imo = t1.imo
    AND t0.quantity < 100
    AND t0.quantity > 0;

ALTER TABLE t_grain RENAME lo_country_code TO port_country;

ALTER TABLE t_grain RENAME lo_city_code TO port_city;

-- process the records with direction as 'X'
DROP TABLE IF EXISTS t_grain_dir_x;

CREATE TEMP TABLE t_grain_dir_x
AS (
    SELECT
        b.draught_arrive,
        b.draught_depart,
        a.*
    FROM
        t_grain a
    LEFT JOIN t_asvt_arrival b ON a.imo = b.imo
    AND a.poi = b.poi
    AND a.date_arrive = b.date_arrive
WHERE
    upper(btrim(direction)) = 'X');

-- positive draught change --> LOAD.
UPDATE
    t_grain
SET
    direction = 'LOAD'
WHERE
    rec_id IN (
        SELECT
            rec_id
        FROM
            t_grain_dir_x
        WHERE
            draught_arrive < draught_depart);

-- negative draught change --> DISCHARGE
UPDATE
    t_grain
SET
    direction = 'DISCHARGE'
WHERE
    rec_id IN (
        SELECT
            rec_id
        FROM
            t_grain_dir_x
        WHERE
            draught_arrive > draught_depart);

/*
zero-draught change cases is added here.
 */


-- update t_grain set next_port = null where btrim(upper(next_port)) in ('TBN', 'N/A', 'TBA', 'TBC');
-- update t_grain set previous_port = null where btrim(upper(previous_port)) in ('TBN', 'N/A');

-- Table to hold all voyages.
drop table if exists t_grain_voyage;
create temp table t_grain_voyage as (
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi poi_depart,
       date_arrive,
       poi poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY CITY'::TEXT note
from t_grain
limit 0);

-- The table is to hold discarded records through the process.
drop table if exists dev.zxw_global_grain_voyage_discard;
create table dev.zxw_global_grain_voyage_discard as (
select a.*,
       'DISCARD REASON'::text discard_reason
from t_grain_voyage a
limit 0);


-- join the grain_voyage_x table.
insert into t_grain_voyage
select a.rec_id,
       a.vessel,
       a.imo,
       a.vessel_id,
       a.operation,
       a.operation_date,
       a.quantity,
       a.grade,
       a.unit,
       a.direction,
       a.port,
       a.port_country,
       a.port_city,
       case
              when upper(btrim(a.direction)) = 'LOAD' then a.date_depart
              when upper(btrim(a.direction)) = 'DISCHARGE' then b.date_depart
       end date_depart,
       case
              when upper(btrim(a.direction)) = 'LOAD' then a.poi
              when upper(btrim(a.direction)) = 'DISCHARGE' then b.poi
       end poi_depart,
       case
              when upper(btrim(a.direction)) = 'LOAD' then b.date_arrive
              when upper(btrim(a.direction)) = 'DISCHARGE' then a.date_arrive
       end date_arrive,
       case
              when upper(btrim(a.direction)) = 'LOAD' then b.poi
              when upper(btrim(a.direction)) = 'DISCHARGE' then a.poi
       end poi_arrive,
       a.charterer,
       a.supplier,
       a.receiver,
       a.source_file,
       a.source_sheet,
       a.report_date,
       a.archive_time,
       current_timestamp update_time,
       'FIND LOAD/DISCH BY X_TABLE'::text note
from t_grain a
join dev.zxw_global_grain_voyage_x b on a.imo = b.imo
                             and a.operation_date = b.operation_date
                             and upper(btrim(a.port)) = upper(btrim(b.port));

/* ------------------
Start to find voyage
--------------------*/

/*
I. Given loading point, find discharge point.
*/

drop table if exists t_grain_load;
create temp table t_grain_load as (
select b.country_un dis_country_decl,
       b.code_un dis_city_decl,
       a.*
from t_grain a
left join lookup.iss_vl_alias_port b on upper(btrim(a.next_port)) = b.name
where upper(btrim(direction)) = 'LOAD'
);

alter table t_grain_load drop column date_arrive;
alter table t_grain_load rename poi to poi_depart;


/*
1.1 find discharge point based on declared city.
(1) same country and city code as declared.
(2) poi can handle R
(3) negative draught change
*/

-- version 1: take draught change into consideration
drop table if exists t_find_disch_by_city_draft;
create temp table t_find_disch_by_city_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo                        
where b.lo_country_code = a.dis_country_decl
  and b.lo_city_code = a.dis_city_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Capesize
    end)
),
t_first_arr as (
-- use to filter multiple discharge.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_first_arr b on a.rec_id = b.rec_id
where a.date_arrive <= (b.date_arrive + interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY CITY WITH DRAFT'::text note
from t_find_disch_by_city_draft;

-- version 2: without considering draft change.

drop table if exists t_find_disch_by_city_no_draft;
create temp table t_find_disch_by_city_no_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo                        
where b.lo_country_code = a.dis_country_decl
  and b.lo_city_code = a.dis_city_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and a.rec_id not in (select distinct rec_id from t_grain_voyage)
)

-- only consider the first arrival due to draft change is not considered.
select * from t0 where row_num = 1
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY CITY WITHOUT DRAFT'::text note
from t_find_disch_by_city_no_draft;


/*
1.2 find discharge point based on declared country.
(1) same country code as declared.
(2) poi can handle R
(3) negative draught change
*/

-- version 1: with draft
drop table if exists t_find_disch_by_country_draft;
create temp table t_find_disch_by_country_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo
where b.lo_country_code = a.dis_country_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Capesize
    end)
  and rec_id not in (select distinct rec_id from t_grain_voyage)
),
t_first_arr as (
-- use to filter multiple discharge.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_first_arr b on a.rec_id = b.rec_id
where a.date_arrive <= (b.date_arrive + interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY COUNTRY WITH DRAFT'::text note
from t_find_disch_by_country_draft;

-- version 2: without draft

drop table if exists t_find_disch_by_country_no_draft;
create temp table t_find_disch_by_country_no_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo
where b.lo_country_code = a.dis_country_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and rec_id not in (select distinct rec_id from t_grain_voyage)
)

select * from t0 where row_num = 1
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY COUNTRY WITHOUT DRAFT'::text note
from t_find_disch_by_country_no_draft;



/*
1.3 find discharge point based on declared region.
(1) same region as declared.
(2) poi can handle R
(3) negative draught change
*/

--version 1: with draft change

drop table if exists t_find_disch_by_region_draft;
create temp table t_find_disch_by_region_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city,
       d.region region_decl,
       e.region arr_region
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo
left join country d on d.un_code = a.dis_country_decl
left join country e on e.un_code = b.lo_country_code
where d.region = e.region
  and dis_country_decl is not null
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Capesize
    end)
  and rec_id not in (select distinct rec_id from t_grain_voyage)
),
t_first_arr as (
-- use to filter multiple discharge.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_first_arr b on a.rec_id = b.rec_id
where a.date_arrive <= (b.date_arrive + interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY REGION WITH DRAFT'::text note
from t_find_disch_by_region_draft;

-- version 2: without draft change 

drop table if exists t_find_disch_by_region_no_draft;
create temp table t_find_disch_by_region_no_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city,
       d.region region_decl,
       e.region arr_region
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo
left join country d on d.un_code = a.dis_country_decl
left join country e on e.un_code = b.lo_country_code
where d.region = e.region
  and dis_country_decl is not null
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and rec_id not in (select distinct rec_id from t_grain_voyage)
)

select * from t0 where row_num = 1
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY REGION WITHOUT DRAFT'::text note
from t_find_disch_by_region_no_draft;

/*
1.4 find discharge point based on first negative draught change.
(1) negative draught change
(2) poi can handle R
(3) first record meeting the above two requirements.
*/

-- version 1: with draft change
drop table if exists t_find_disch_by_first_arr;
create temp table t_find_disch_by_first_arr as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart + interval '1 day' and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo                        
where b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Capesize
    end)
and rec_id not in (select distinct rec_id from t_grain_voyage))

select * from t0
where row_num = 1
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY 1ST ARRIVAL WITH DRAFT'::text note
from t_find_disch_by_first_arr;

-- version 2: without draft change

drop table if exists t_find_disch_by_first_arr_no_draft;
create temp table t_find_disch_by_first_arr_no_draft as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) row_num,
       a.*,
       b.date_arrive,
       b.poi poi_arrive,
       b.lo_country_code arr_country,
       b.lo_city_code arr_city
from t_grain_load a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive between a.date_depart + interval '1 day' and a.date_depart + interval '80 days'
left join tanker c on a.imo = c.imo                        
where b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
and rec_id not in (select distinct rec_id from t_grain_voyage))

select * from t0
where row_num = 1
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY 1ST ARRIVAL WITHOUT DRAFT'::text note
from t_find_disch_by_first_arr_no_draft;



/*
Find voyages between date_depart and first arrival found by previous logic.
If there exists one, add the arrival into voyage table.
*/

drop table if exists t_find_disch_by_mid_arr;
create temp table t_find_disch_by_mid_arr as (
with t0 as (
select row_number() over(partition by rec_id order by date_arrive) arr_num,
       *
from t_grain_voyage
)

select b.lo_country_code,
       b.lo_city_code,
       b.poi poi_arrive_between,
       b.date_arrive date_arrive_between,
       b.draught_arrive,
       b.draught_depart,
       a.* 
from t0 a
left join t_asvt_arrival b on b.date_arrive < a.date_arrive
                          and b.date_arrive > a.date_depart
                          and a.imo = b.imo
left join tanker c on a.imo = c.imo                          
where a.arr_num = 1
and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.08*b.draught_arrive -- Capesize
    end)
and b.lo_country_code <> a.port_country
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       lo_country_code port_country,
       lo_city_code port_city,
       date_depart,
       poi_depart,
       date_arrive_between date_arrive,
       poi_arrive_between poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND DISCHARGE BY ARR BETWEEN DEP AND 1ST ARR'::text note
from t_find_disch_by_mid_arr;


/*
When total draught change is negative for a vessel,
remove arrivals with zero draught change.
*/

drop table if exists t_fake_multi_disch;
create temp table t_fake_multi_disch as (
with t0 as (
select a.*,
       b.draught_arrive,
       b.draught_depart
from t_grain_voyage a
left join t_asvt_arrival b on a.imo = b.imo and
                              a.date_arrive = b.date_arrive and
                              a.poi_arrive = b.poi
),
t1 as (
select rec_id,
       count(*) arr_num,
       sum(draught_depart - draught_arrive) total_draught_change
from t0
group by rec_id
),
t2 as (
select * from t1
where arr_num > 1 and total_draught_change < 0
order by arr_num desc)

select * from t0
where rec_id in (select rec_id from t2) and
      draught_depart = draught_arrive
order by rec_id, operation_date
);

alter table t_fake_multi_disch drop column draught_arrive, drop column draught_depart;

-- Throw fake multi arrivals into discard table.
insert into dev.zxw_global_grain_voyage_discard
select a.*,
       'FAKE MULTI DISCH'::text discard_reason
from t_fake_multi_disch a;


-- delete fake arrivals from voyage table.
DELETE
FROM t_grain_voyage
WHERE (rec_id,
       poi_arrive,
       date_arrive) IN
    (SELECT rec_id,
            poi_arrive,
            date_arrive
     FROM t_fake_multi_disch);

/*
II. Given discharge point, find loading point.
*/

drop table if exists t_grain_disch;
create temp table t_grain_disch as (
select b.country_un load_country_decl,
       b.code_un load_city_decl,
       a.*
from t_grain a
left join lookup.iss_vl_alias_port b on upper(btrim(a.previous_port)) = b.name
where upper(btrim(direction)) = 'DISCHARGE'
);


alter table t_grain_disch drop column date_depart;
alter table t_grain_disch rename poi to poi_arrive;

/*
2.1 find load point based on declared city.
(1) same country and city code as declared.
(2) poi can handle R
(3) positive draught change
*/

drop table if exists t_find_load_by_city;
create temp table t_find_load_by_city as (
with t0 as (
select row_number() over(partition by rec_id order by b.date_arrive desc) row_num,
       a.*,
       b.date_depart,
       b.poi poi_depart,
       b.lo_country_code depart_country,
       b.lo_city_code depart_city
from t_grain_disch a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive >= (a.date_arrive - interval '80 days')
                        and b.date_arrive < a.date_arrive
where b.lo_country_code = a.load_country_decl
  and b.lo_city_code = a.load_city_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%L%')
  and b.draught_arrive < b.draught_depart
),
t_last_depart as (
-- use to filter multiple loadings.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_last_depart b on a.rec_id = b.rec_id
where a.date_depart >= (b.date_depart - interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND LOAD BY CITY'::text note
from t_find_load_by_city;

/*
2.2 find load point based on declared country.
(1) same country code as declared.
(2) poi can handle R
(3) positive draught change
*/
drop table if exists t_find_load_by_country;
create temp table t_find_load_by_country as (
with t0 as (
select row_number() over(partition by rec_id order by b.date_arrive desc) row_num,
       a.*,
       b.date_depart,
       b.poi poi_depart,
       b.lo_country_code depart_country,
       b.lo_city_code depart_city
from t_grain_disch a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive >= (a.date_arrive - interval '80 days')
                        and b.date_arrive < a.date_arrive
where b.lo_country_code = a.load_country_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%L%')
  and b.draught_arrive < b.draught_depart
  and rec_id not in (select distinct rec_id from t_grain_voyage)
),
t_last_depart as (
-- use to filter multiple loadings.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_last_depart b on a.rec_id = b.rec_id
where a.date_depart >= (b.date_depart - interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND LOAD BY COUNTRY'::text note
from t_find_load_by_country;


/*
2.3 find load point based on declared region.
(1) same region code as declared.
(2) poi can handle R
(3) positive draught change
*/

drop table if exists t_find_load_by_region;
create temp table t_find_load_by_region as (
with t0 as (
select row_number() over(partition by rec_id order by b.date_arrive desc) row_num,
       a.*,
       b.date_depart,
       b.poi poi_depart,
       b.lo_country_code depart_country,
       b.lo_city_code depart_city
from t_grain_disch a
left join t_asvt_arrival b on a.imo = b.imo
                        and b.date_arrive >= (a.date_arrive - interval '80 days')
                        and b.date_arrive < a.date_arrive
left join country d on d.un_code = a.load_country_decl
left join country e on e.un_code = b.lo_country_code
where d.region = e.region
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%L%')
  and b.draught_arrive < b.draught_depart
  and rec_id not in (select distinct rec_id from t_grain_voyage)
),
t_last_depart as (
-- use to filter multiple discharge.
select * from t0 where row_num = 1
)

select a.* from t0 a
left join t_last_depart b on a.rec_id = b.rec_id
where a.date_depart >= (b.date_depart - interval '10 day')
);

insert into t_grain_voyage
select rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND LOAD BY REGION'::text note
from t_find_load_by_region;


/*
2.4 find load point based on first positive draught change.
(1) positive draught change
(2) poi can handle R
(3) first record meeting the above two requirements.
*/

drop table if exists t_find_load_by_first_pos;
create temp table t_find_load_by_first_pos as (
with t0 as (
SELECT row_number() over(partition BY rec_id ORDER BY b.date_arrive DESC) row_num,
       a.*,
       b.date_depart,
       b.poi poi_depart,
       b.lo_country_code depart_country,
       b.lo_city_code depart_city
from t_grain_disch a
left join t_asvt_arrival b on a.imo = b.imo
                          and b.date_arrive >= (a.date_arrive - interval '80 days')
                          and b.date_arrive < a.date_arrive
where b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%L%')
  and b.draught_arrive < b.draught_depart
  and rec_id not in (select distinct rec_id from t_grain_voyage))

select * from t0
where row_num = 1
);

INSERT INTO t_grain_voyage
SELECT rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi_depart,
       date_arrive,
       poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'FIND LOAD BY 1ST POS DRAUGHT CHANGE DEPART'::text note
FROM t_find_load_by_first_pos;

/* REMOVE DUPLICATED ROWS FROM T_GRAIN_VOYAGE.
   Note: at the very beginning, rec_id is assigned to each record
   as the unique id. Therefore, rec_id could be used here for
   removing duplicates.
*/

-- throw duplicated voyages into discard table.
insert into dev.zxw_global_grain_voyage_discard
select a.*,
       'DUPLICATED VOYAGE'::text discard_reason
FROM t_grain_voyage a
WHERE rec_id NOT IN
    ( SELECT min_rec_id
      FROM
       ( SELECT vessel,
                imo,
                quantity,
                grade,
                port,
                date_depart,
                poi_depart,
                date_arrive,
                poi_arrive,
                min(rec_id) min_rec_id
        FROM t_grain_voyage
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9) t0);

delete from t_grain_voyage where rec_id in (
select rec_id from dev.zxw_global_grain_voyage_discard
where discard_reason = 'DUPLICATED VOYAGE'
);


/*
 * FIND OPEN VOYAGES
 * Given the depart date, if there is no arrival(s)
 * after the departure, which means the vessel is en-route,
 * then the record will be set as an open voyage.
 */


WITH t0 AS
  (SELECT *
   FROM t_grain
   WHERE rec_id NOT IN
       (SELECT rec_id
        FROM t_grain_voyage)
     AND upper(btrim(direction)) = 'LOAD'
     AND date_depart >= CURRENT_DATE - interval '45 days'
   ORDER BY date_depart),
     t1 AS
  (SELECT b.imo,
          c.lo_country_code,
          c.lo_city_code,
          a.*
   FROM asvt_arrival a
   LEFT JOIN as_vessel_exp b ON a.vessel = b.vessel
   LEFT JOIN as_poi c ON a.poi = c.poi
   WHERE imo IN
       (SELECT DISTINCT imo
        FROM t0)),
     t2 AS
  (SELECT a.*,
          b.lo_country_code,
          b.poi,
          b.date_depart
   FROM t0 a
   LEFT JOIN t1 b ON a.imo = b.imo
   AND a.date_depart <= b.date_depart
   ORDER BY rec_id),
     t3 AS
  (SELECT rec_id,
          count(*)
   FROM t2
   GROUP BY rec_id HAVING count(*) <= 1)
INSERT INTO t_grain_voyage
SELECT
       rec_id,
       vessel,
       imo,
       vessel_id,
       operation,
       operation_date,
       quantity,
       grade,
       unit,
       direction,
       port,
       port_country,
       port_city,
       date_depart,
       poi poi_depart,
       null::timestamp without time zone date_arrive,
       null::integer poi_arrive,
       charterer,
       supplier,
       receiver,
       source_file,
       source_sheet,
       report_date,
       archive_time,
       current_timestamp update_time,
       'OPEN VOYAGE'::text note
FROM t0
WHERE rec_id IN
    (SELECT rec_id
     FROM t3 );

/*
REMOVE ARRIVALS AFTER NEXT LOADINGS.

Shuttle vessels could load and discharge between two places multiple times
in a short period, therefore, arrivals of next voyages may be included as 
the arrivals of current voyages. Under such circumstances, too many arrivals
are found, so the following logic is used to handle such situation. 
*/

DROP TABLE IF EXISTS t_arr_more_than_1;


CREATE TEMP TABLE t_arr_more_than_1 AS
  (SELECT row_number() over(partition BY rec_id
                            ORDER BY date_arrive) row_num,
          a.*
   FROM t_grain_voyage a
   WHERE rec_id IN (
                      (SELECT rec_id
                       FROM t_grain_voyage
                       GROUP BY 1 HAVING count(*) > 3)));

 -- Remove fake voyages and insert right voyages later.

DELETE
FROM t_grain_voyage
WHERE rec_id IN
    (SELECT DISTINCT rec_id
     FROM t_arr_more_than_1);


DROP TABLE IF EXISTS t_arr_after_load;


CREATE TEMP TABLE t_arr_after_load AS ( WITH t1 AS
(SELECT coalesce(lag(date_arrive) over(partition BY rec_id
                                      ORDER BY date_arrive), 
                date_arrive) prev_date_arrive,
       a.*
FROM t_arr_more_than_1 a)
SELECT b.date_arrive date_arrive_between,
       b.poi poi_arrive_between,
       b.draught_arrive draught_arrive_between,
       b.draught_depart draught_depart_between,
       a.*
FROM t1 a
LEFT JOIN t_asvt_arrival b ON a.imo = b.imo
                          AND b.date_arrive > a.prev_date_arrive
                          AND b.date_arrive < a.date_arrive
WHERE b.draught_arrive < b.draught_depart -- a loading
);


DELETE
FROM t_arr_more_than_1
WHERE (rec_id,
       row_num) IN
    ( SELECT a.rec_id,
             a.row_num
     FROM t_arr_more_than_1 a
     LEFT JOIN
       ( SELECT rec_id,
                min(row_num) min_row_num
        FROM t_arr_after_load
        GROUP BY rec_id
        ORDER BY 2) b ON a.rec_id = b.rec_id
     WHERE a.row_num >= b.min_row_num);

 -- Remove row_num column.

ALTER TABLE t_arr_more_than_1
DROP COLUMN row_num;

 -- Update note column

UPDATE t_arr_more_than_1
SET note = note||'--REVISED';


INSERT INTO t_grain_voyage
SELECT *
FROM t_arr_more_than_1;

/*
SPLITTING QUANTITY FOR MULTIPLE ARRIVALS.
*/
WITH t_div AS ((
        SELECT
            rec_id,
            count(*)
            div
        FROM
            t_grain_voyage
        GROUP BY
            rec_id
        HAVING
            count(*) > 1))
UPDATE
    t_grain_voyage a
SET
    quantity = round(quantity / b.div, 0)
FROM
    t_div b
WHERE
    a.rec_id IN ((
            SELECT
                rec_id
            FROM
                t_grain_voyage
            GROUP BY
                rec_id
            HAVING
                count(*) > 1))
        AND a.rec_id = b.rec_id;




/**
 * Write results into tables.
 */

/* Store unmatched records into grain_unmatch*/
-- DROP TABLE IF EXISTS dev.zxw_grain_unmatch_arr;
-- CREATE TABLE dev.zxw_grain_unmatch_arr AS
--   (SELECT *
--    FROM t_grain_load
--    WHERE rec_id NOT IN
--        (SELECT rec_id
--         FROM t_grain_voyage)
--      and rec_id not in (
--   select rec_id from dev.zxw_global_grain_voyage_discard
--   where discard_reason = 'DUPLICATED VOYAGE'
--      )
--    ORDER BY rec_id);

DELETE FROM  dev.zxw_grain_unmatch_arr;
INSERT INTO dev.zxw_grain_unmatch_arr
  (SELECT *
   FROM t_grain_load
   WHERE rec_id NOT IN
       (SELECT rec_id
        FROM t_grain_voyage)
     and rec_id not in (
  select rec_id from dev.zxw_global_grain_voyage_discard
  where discard_reason = 'DUPLICATED VOYAGE'
     )
   ORDER BY rec_id);


/* Store unmatched records into grain_unmatch*/
-- DROP TABLE IF EXISTS dev.zxw_grain_unmatch_dep;
-- CREATE TABLE dev.zxw_grain_unmatch_dep AS
--   (SELECT *
--    FROM t_grain_disch
--    WHERE rec_id NOT IN
--        (SELECT rec_id
--         FROM t_grain_voyage)
--      and rec_id not in (
--   select rec_id from dev.zxw_global_grain_voyage_discard
--   where discard_reason = 'DUPLICATED VOYAGE'
--      )
--    ORDER BY rec_id);


DELETE FROM dev.zxw_grain_unmatch_dep;
INSERT INTO  dev.zxw_grain_unmatch_dep
  (SELECT *
   FROM t_grain_disch
   WHERE rec_id NOT IN
       (SELECT rec_id
        FROM t_grain_voyage)
     and rec_id not in (
  select rec_id from dev.zxw_global_grain_voyage_discard
  where discard_reason = 'DUPLICATED VOYAGE'
     )
   ORDER BY rec_id);



-- remove old data from global_grain_voyage
-- ALTER TABLE t_grain_voyage
-- DROP COLUMN rec_id;

DELETE FROM  dev.zxw_global_grain_voyage;
 -- insert newly matched voyages into global_grain_voyage
INSERT INTO dev.zxw_global_grain_voyage
select a.rec_id,
       a.vessel,
       a.imo,
       a.vessel_id,
       a.operation,
       a.operation_date,
       (case
           when a.quantity = 0 then b.dwt
           else a.quantity
        end) quantity,
       a.grade,
       a.unit,
       a.direction,
       a.port,
       a.port_country,
       a.port_city,
       a.date_depart,
       a.poi_depart,
       a.date_arrive,
       a.poi_arrive,
       a.charterer,
       a.supplier,
       a.receiver,
       a.source_file,
       a.source_sheet,
       a.report_date,
       a.archive_time,
       a.update_time,
       a.note
from t_grain_voyage a
left join tanker b on a.imo = b.imo;


/**
 * MATCH COVERAGE STATISTICS
 */

 WITH t_count_total AS
  (SELECT count(*) count_total
   FROM t_grain),
      t_count_matched AS
  (SELECT count(distinct(rec_id)) count_matched
   FROM t_grain_voyage)
INSERT INTO dev.zxw_global_grain_voyage_stat
SELECT now() date_run,
       count_total,
       count_matched,
       round(count_matched::numeric/count_total, 4) coverage
FROM t_count_total,
     t_count_matched;


END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION dev.zxw_f_find_global_grain_voyage()
  OWNER TO xiao;