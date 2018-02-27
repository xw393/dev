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
drop table if exists t_asvt_arrival;
create temp table t_asvt_arrival as (
select b.imo,
       c.lo_country_code,
       c.lo_city_code,
       a.*
from asvt_arrival a
left join as_vessel_exp b on a.vessel = b.vessel
left join as_poi c on a.poi = c.poi);

create index indx_imo_poi_date_arrive_asvt_arrival on t_asvt_arrival (imo, poi, date_arrive);


-- Get grain data from inchcape.iss_process_2.
drop table if exists t_grain;
create temp table t_grain as (
select ((row_number() over w) + 10000)::int rec_id,
       a.*
from inchcape.iss_process_2 a
where grade in (select code from t_cat_prod where cmdty = 'R')
window w as (order by vessel, operation_date)
);

-- update quantity values if quantity is less than 100.
update t_grain as t0
set quantity = t1.revised_quantity
from (select b.dwt,
       (case
		when a.quantity < b.dwt/1000. then a.quantity*1000
		else b.dwt
       end) revised_quantity,	
       a.* 
from t_grain a
left join tanker b on a.imo = b.imo
where quantity < 100 and quantity > 0
order by quantity) as t1
where t0.imo = t1.imo and t0.quantity < 100 and t0.quantity > 0;


alter table t_grain rename lo_country_code to port_country;
alter table t_grain rename lo_city_code to port_city;

-- process the records with direction as 'X'
drop table if exists t_grain_dir_x;
create temp table t_grain_dir_x as (
select b.draught_arrive,
       b.draught_depart,
       a.*
from t_grain a
left join t_asvt_arrival b on a.imo = b.imo
      and a.poi = b.poi
      and a.date_arrive = b.date_arrive
where upper(btrim(direction)) = 'X'
);

-- positive draught change --> LOAD.
update t_grain
set direction = 'LOAD'
where rec_id in (
select rec_id from t_grain_dir_x
where draught_arrive < draught_depart
);

-- negative draught change --> DISCHARGE
update t_grain
set direction = 'DISCHARGE'
where rec_id in (
select rec_id from t_grain_dir_x
where draught_arrive > draught_depart
);

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
drop table if exists t_find_disch_by_city;
create temp table t_find_disch_by_city as (
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
where b.lo_country_code = a.dis_country_decl
  and b.lo_city_code = a.dis_city_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and 1.02*b.draught_arrive >= b.draught_depart
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
       'FIND DISCHARGE BY CITY'::text note
from t_find_disch_by_city;

/*
1.2 find discharge point based on declared country.
(1) same country code as declared.
(2) poi can handle R
(3) negative draught change
*/

drop table if exists t_find_disch_by_country;
create temp table t_find_disch_by_country as (
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
where b.lo_country_code = a.dis_country_decl
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and 1.02*b.draught_arrive >= b.draught_depart
  and rec_id not in (select rec_id from t_grain_voyage)
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
       'FIND DISCHARGE BY COUNTRY'::text note
from t_find_disch_by_country;

/*
1.3 find discharge point based on declared region.
(1) same region as declared.
(2) poi can handle R
(3) negative draught change
*/

drop table if exists t_find_disch_by_region;
create temp table t_find_disch_by_region as (
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
left join country d on d.un_code = a.dis_country_decl
left join country e on e.un_code = b.lo_country_code
where d.region = e.region
  and dis_country_decl is not null
  and b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  and 1.02*b.draught_arrive >= b.draught_depart
  and rec_id not in (select rec_id from t_grain_voyage)
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
       'FIND DISCHARGE BY REGION'::text note
from t_find_disch_by_region;

/*
1.4 find discharge point based on first negative draught change.
(1) negative draught change
(2) poi can handle R
(3) first record meeting the above two requirements.
*/

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
where b.poi in (select poi from poi_dir where cmdty = 'R' and loadunl ilike '%U%')
  -- and 1.015*b.draught_arrive >= b.draught_depart
  and rec_id not in (select rec_id from t_grain_voyage))

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
       'FIND DISCHARGE BY 1ST ARRIVAL'::text note
from t_find_disch_by_first_arr;



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
-- where imo = 9535876
-- and operation_date = '2017-06-09'::date
-- and grade = 'ED033'
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
and (case -- the ratio is estimated by calculating avg(draught_change)/avg(draught_arrive)
	when c.dwt < 10000 then (b.draught_arrive - b.draught_depart) >= 0.1 -- shuttle vessel
	when c.dwt >= 10000 and c.dwt < 35000 then (b.draught_arrive - b.draught_depart) > 0.2*b.draught_arrive -- Handysize
	when c.dwt >= 35000 and c.dwt < 60000 then (b.draught_arrive - b.draught_depart) > 0.2*b.draught_arrive -- Handymax
	when c.dwt >= 60000 and c.dwt < 80000 then (b.draught_arrive - b.draught_depart) > 0.2*b.draught_arrive -- Panamax
	else (b.draught_arrive - b.draught_depart) > 0.2*b.draught_arrive -- Capesize
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



select * from t_find_disch_by_mid_arr
where imo = 9535876
and operation_date = '2017-06-09'

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
  and rec_id not in (select rec_id from t_grain_voyage))

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
TODO: add quantity splitting logic here for multiple arrivals.
*/



/**
 * Write results into tables.
 */

/* Store unmatched records into grain_unmatch*/
DROP TABLE IF EXISTS dev.zxw_grain_unmatch_arr;
CREATE TABLE dev.zxw_grain_unmatch_arr AS
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
DROP TABLE IF EXISTS dev.zxw_grain_unmatch_dep;
CREATE TABLE dev.zxw_grain_unmatch_dep AS
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

TRUNCATE TABLE dev.zxw_global_grain_voyage;
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