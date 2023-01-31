-- noinspection SqlNoDataSourceInspectionForFile

drop TABLE if exists adds_temp.demo_songs_{format_code} ;;
drop TABLE if exists adds_temp.demo_song_subset_train_{format_code} ;;
drop TABLE if exists adds_temp.demo_station_song_subset_train_{format_code};;
drop TABLE if exists adds_temp.demo_song_subset_score_{format_code} ;;
drop TABLE if exists adds_temp.demo_station_song_subset_score_{format_code};;
drop TABLE if exists adds_temp.demo_gcr_{format_code} ;;
drop TABLE if exists adds_temp.demo_cm_{format_code};;
drop TABLE if exists adds_temp.demo_mb_{format_code};;
drop table if exists adds_temp.demo_similar_station_{format_code};;
drop table if exists adds_temp.demo_rr_temp_{format_code};;
drop table if exists adds_temp.demo_rr_features_{format_code};;
DROP TABLE IF EXISTS adds_temp.demo_sim_stations_{format_code};;
DROP TABLE IF EXISTS adds_temp.demo_smoothed_pop_score_{format_code};



-- songs
create table adds_temp.demo_songs_{format_code} as (
    select distinct
        s.artist_name as "Artist",  --"FirstLast",
        s.song_name as "Title",     -- "SongTitle",
        s.mediabase_id as "SongID"
        --trim((regexp_split_to_array( regexp_replace(upper(artist_name),'[^[A-Z0-9,&\/\s]|[[THE ]]','','g'),'(F\/.*)|(W\/.*)|[,&\/]'))[1]) as sm_artist,
        --trim((regexp_split_to_array( regexp_replace(upper(song_name),'[^[A-Z0-9,&\/\s]','','g'),'(F\/.*)|(W\/.*)|[,&\/]'))[1]) as sm_title
    from dbo."MediabaseSongSpins" mb
    join data.stations_v st
        on st.call_letters = mb."C_Let"
    join data.songs_v s
        on mb."SongID"= s.mediabase_id
    where
        mb."StartDate" in (select max("StartDate") from dbo."MediabaseSongSpins" mss ) and lower(format_code)='{format_code}'
);;


-- song-train-subset
create table adds_temp.demo_song_subset_train_{format_code} as (

select distinct mediabase_id, ds.artist_name, a.artist_id, song_name

from data.cmm c
/*
(
Select c.cmm_station_calls, c.song_id
from data.cmm c
group by 1, 2
having count(c.test_date) >= '{min_station_tests}' # atleast two station tests) as c
*/

join data.stations_v st
on st.call_letters = c.cmm_station_calls

join data.songs_v ds
on c.song_id = ds.song_id

join data.artists a
on a.artist_name = ds.artist_name

left join
(select distinct "SongCode", last_value("GRC") over (partition by "SongCode" order by "EffectiveDate") as "GRC"

from  dbo."Grcxfr" g where lower("FormatCode") ='{format_code}') g
on trim(g."SongCode")= trim(ds.song_code)

left join
(select song_id, station_id, min(week_dt) as first_spin_thresh_week
from (

select "SongID" as song_id, station_id,
case when extract('dow' from "StartDate")=1 then cast(("StartDate" - interval '1 day' ) as date)
 when extract('dow' from "StartDate")=2 then cast(("StartDate" - interval '2 day') as date)
 when extract('dow' from "StartDate")=3 then cast(("StartDate" - interval '3 day') as date)
 when extract('dow' from "StartDate")=4 then cast(("StartDate" - interval '4 day') as date)
 when extract('dow' from "StartDate")=5 then cast(("StartDate" - interval '5 day') as date)
 when extract('dow' from "StartDate")=6 then cast(("StartDate" - interval '6 day') as date)
else cast("StartDate" as date)
end as week_dt,

sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))
over (partition by "SongID","C_Let" order by "StartDate" ) spins_nonon_to_date

from
dbo."MediabaseSongSpins" aa

join data.stations_v st
on st.call_letters = aa."C_Let"

where owner_name='iHeartMedia, Inc.'
and lower(format_code) ='{format_code}'
and market_name <>'>iHM Custom'

) a
where spins_nonon_to_date>{spins_filter}
group by 1,2
) mt

on mt.song_id = ds.mediabase_id
and mt.station_id = st.station_id
--and mt.week_dt =date_trunc('week', c.test_date)::date- '8 day'::interval

where lower(format_code)='{format_code}'
and test_date >= '{start_date}'
and (project_type='Callout' or project_type='Omt')
and breakout_id=1
and (first_spin_thresh_week <= (date_trunc('week', c.test_date)::date- '8 day'::interval)::date or ("GRC"='G' or "GRC" isnull) )
);;


-- song-station-breakout-train-subset
create table adds_temp.demo_station_song_subset_train_{format_code} as (

select distinct cmm_station_calls, mediabase_id, market_name,
ds.artist_name, ds.song_name, ds.song_release_date, a.artist_id,
first_spin_thresh_week, "GRC"

from data.cmm c
/*
(
Select c.cmm_station_calls, c.song_id
from data.cmm c
group by 1, 2
having count(c.test_date) >= 2 # atleast two station tests) as c
 */

join data.stations_v st
on st.call_letters = c.cmm_station_calls

join data.songs_v ds
on c.song_id = ds.song_id

join data.artists a
on a.artist_name = ds.artist_name

left join
(select distinct "SongCode", last_value("GRC") over (partition by "SongCode" order by "EffectiveDate") as "GRC"

from  dbo."Grcxfr" g where lower("FormatCode") ='{format_code}') g
on trim(g."SongCode")= trim(ds.song_code)

left join
(select song_id, station_id, min(week_dt) as first_spin_thresh_week
from (

select "SongID" as song_id, station_id,
case when extract('dow' from "StartDate")=1 then cast(("StartDate" - interval '1 day' ) as date)
 when extract('dow' from "StartDate")=2 then cast(("StartDate" - interval '2 day') as date)
 when extract('dow' from "StartDate")=3 then cast(("StartDate" - interval '3 day') as date)
 when extract('dow' from "StartDate")=4 then cast(("StartDate" - interval '4 day') as date)
 when extract('dow' from "StartDate")=5 then cast(("StartDate" - interval '5 day') as date)
 when extract('dow' from "StartDate")=6 then cast(("StartDate" - interval '6 day') as date)
else cast("StartDate" as date)
end as week_dt,

sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))
over (partition by "SongID","C_Let" order by "StartDate" ) spins_nonon_to_date

from
dbo."MediabaseSongSpins" aa

join data.stations_v st
on st.call_letters = aa."C_Let"

where owner_name='iHeartMedia, Inc.'
and lower(format_code) ='{format_code}'
and market_name <>'>iHM Custom'

) a
where spins_nonon_to_date>{spins_filter}
group by 1,2
) mt

on mt.song_id = ds.mediabase_id
and mt.station_id = st.station_id
--and mt.week_dt =date_trunc('week', c.test_date)::date- '8 day'::interval


where lower(format_code)='{format_code}'
and test_date >= '{start_date}'
and (project_type='Callout' or project_type='Omt')
and breakout_id=1
and (first_spin_thresh_week <= (date_trunc('week', c.test_date)::date- '8 day'::interval)::date or ("GRC"='G' or "GRC" isnull) )
)
;;



create table adds_temp.demo_song_subset_score_{format_code} as (


select distinct mediabase_id, ds.artist_name, a.artist_id, song_name
from data.cmm c

join data.stations_v st
on st.call_letters = c.cmm_station_calls

join data.songs_v ds
on c.song_id = ds.song_id
join data.artists a
on a.artist_name = ds.artist_name
left join
(select distinct "SongCode", last_value("GRC") over (partition by "SongCode" order by "EffectiveDate") as "GRC"

from  dbo."Grcxfr" g where lower("FormatCode") ='{format_code}') g
on trim(g."SongCode")= trim(ds.song_code)


left join
(select song_id, station_id, min(week_dt) as first_spin_thresh_week
from (

select "SongID" as song_id, station_id,
case when extract('dow' from "StartDate")=1 then cast(("StartDate" - interval '1 day' ) as date)
 when extract('dow' from "StartDate")=2 then cast(("StartDate" - interval '2 day') as date)
 when extract('dow' from "StartDate")=3 then cast(("StartDate" - interval '3 day') as date)
 when extract('dow' from "StartDate")=4 then cast(("StartDate" - interval '4 day') as date)
 when extract('dow' from "StartDate")=5 then cast(("StartDate" - interval '5 day') as date)
 when extract('dow' from "StartDate")=6 then cast(("StartDate" - interval '6 day') as date)
else cast("StartDate" as date)
end as week_dt,

sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))
over (partition by "SongID","C_Let" order by "StartDate" ) spins_nonon_to_date

from
dbo."MediabaseSongSpins" aa

join data.stations_v st
on st.call_letters = aa."C_Let"

where owner_name='iHeartMedia, Inc.'
and lower(format_code) ='{format_code}'
and market_name <>'>iHM Custom'

) a
where spins_nonon_to_date>{spins_filter}
group by 1,2
) mt

on mt.song_id = ds.mediabase_id
and mt.station_id = st.station_id
--and mt.week_dt =date_trunc('week', c.test_date)::date- '8 day'::interval

where lower(format_code)='{format_code}'
and ((test_date >= cast('{start_date}' as date)-interval '6 months' and test_date <= cast('{start_date}' as date)+interval '13 days' and project_type='Callout')
    or (test_date >= cast('{start_date}' as date)-interval '24 months' and test_date <= cast('{start_date}' as date)+interval '13 days'  and project_type='Omt' and ("GRC"='G' or "GRC" isnull)  ))
and breakout_id=1
and (first_spin_thresh_week <= (date_trunc('week', c.test_date)::date- '8 day'::interval)::date or ("GRC"='G' or "GRC" isnull) )

);;


create table adds_temp.demo_station_song_subset_score_{format_code} as (
select distinct "C_Let" as cmm_station_calls,
ss.mediabase_id,
market_name,
ds.artist_name, ds.song_name, ds.song_release_date, a.artist_id from

(
select distinct "SongID", "C_Let"
from dbo."MediabaseSongSpins" mb

join data.stations_v st
on st.call_letters = mb."C_Let"

where
lower(format_code)='{format_code}'
and "StartDate" between (cast('{score_date}' as date)-interval '3 months') and (cast('{score_date}' as date)+interval '13 days')
group by 1,2
 ) aa

join
adds_temp.demo_song_subset_score_{format_code} ss
on aa."SongID"= ss.mediabase_id

join data.stations_v st
on st.call_letters = aa."C_Let"

join data.songs_v ds
on ss.mediabase_id = ds.mediabase_id

join data.artists a
on a.artist_name = ds.artist_name

where owner_name='iHeartMedia, Inc.'
and lower(format_code) ='{format_code}'
and market_name <>'>iHM Custom'
)
;;



-- misc formatting
ALTER TABLE adds_temp.demo_songs_{format_code} ADD PRIMARY KEY ("Artist", "Title", "SongID");;
ALTER TABLE adds_temp.demo_song_subset_train_{format_code} ADD PRIMARY KEY (mediabase_id);;
ALTER TABLE adds_temp.demo_station_song_subset_train_{format_code} ADD PRIMARY KEY ( cmm_station_calls, mediabase_id );;
ALTER TABLE adds_temp.demo_song_subset_score_{format_code}  ADD PRIMARY KEY (mediabase_id);;
ALTER TABLE adds_temp.demo_station_song_subset_score_{format_code}  ADD PRIMARY KEY ( cmm_station_calls, mediabase_id );;


-- Mediabase weekly spins data
create table adds_temp.demo_mb_{format_code} as (

select *,
sum(case when station_market_rank=1 then market_spins else 0 end) over (partition by "SongID",week_dt) as song_univ_spins,
sum(case when station_market_rank=1 then market_spins else 0 end) over (partition by "FirstLast",week_dt) as artist_univ_spins


from

(
select mb."SongID",
mb."C_Let",
st.station_id,
market_name as "Market_Name",
artist_name as "FirstLast", song_name as "SongTitle",
case when extract('dow' from "StartDate")=1 then cast(("StartDate" - interval '1 day' ) as date)
 when extract('dow' from "StartDate")=2 then cast(("StartDate" - interval '2 day') as date)
 when extract('dow' from "StartDate")=3 then cast(("StartDate" - interval '3 day') as date)
 when extract('dow' from "StartDate")=4 then cast(("StartDate" - interval '4 day') as date)
 when extract('dow' from "StartDate")=5 then cast(("StartDate" - interval '5 day') as date)
 when extract('dow' from "StartDate")=6 then cast(("StartDate" - interval '6 day') as date)
else cast("StartDate" as date)
end as week_dt,

sum(coalesce("DP1",0)+coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))  over (partition by mb."SongID",
mb."C_Let", "StartDate")  as spins_total,
sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))   over (partition by mb."SongID",
mb."C_Let", "StartDate") as spins_non_on,
sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0))   over (partition by mb."SongID",
mb."C_Let", "StartDate") as spins_am_pm_dr,

"MarketSpinsToDate" as market_spins,
"SpinsToDate" as station_spins,
sum("SpinsToDate") over (partition by mb."SongID", "StartDate") as format_spins,
sum("SpinsToDate") over (partition by artist_name, "StartDate") as format_artist_spins,
sum("SpinsToDate") over (partition by "C_Let",artist_name, "StartDate") as station_artist_spins,
sum("MarketSpinsToDate") over (partition by "C_Let",artist_name, "StartDate") as market_artist_spins,
dense_rank() over (partition by mb."SongID", "StartDate",market_name  order by "SpinsToDate") as station_market_rank

from


(select coalesce("SongID", b.mediabase_id ) as "SongID",
coalesce("C_Let",b.cmm_station_calls ) as "C_Let",
coalesce("SpinsToDate",0) as "SpinsToDate",
coalesce("MarketSpinsToDate",0) as "MarketSpinsToDate",
coalesce("DP1",0) as "DP1",
coalesce("DP2",0) as "DP2",
coalesce("DP3",0) as "DP3",
coalesce("DP4",0) as "DP4",
coalesce("DP5",0) as "DP5",
coalesce("StartDate", week_dt) as "StartDate",
coalesce("StartDate", week_dt) + interval '6 days' as "EndDate"

from dbo."MediabaseSongSpins" mb

--adding in zero spin weeks
right join
(select *, day as week_dt from adds_temp.demo_station_song_subset_{train_score}_{format_code}

cross join (
SELECT day::date
FROM   generate_series(date '2016-01-03', current_date, '7 day') day
where  day > '{start_date}'

) a

) b

on mb."C_Let"=b.cmm_station_calls
and mb."SongID" = b.mediabase_id
and mb."StartDate" = b.week_dt) mb


join data.stations_v st
on st.call_letters = mb."C_Let"
join data.songs_v ds
on mb."SongID" = ds.mediabase_id


where
lower(format_code)='{format_code}' and
"StartDate">'{start_date}' --and "StartDate"<= cast('{score_date}' as date)+interval '6 days'

union all


select *,
dense_rank() over (partition by "SongID","Market_Name", week_dt  order by station_spins) as station_market_rank
from
(
select distinct mb."SongID",
mb."C_Let",
st.station_id,
market_name as "Market_Name",
artist_name as "FirstLast", song_name as "SongTitle",
case when extract('dow' from cast('{start_date}' as date))=1 then cast((cast('{start_date}' as date) - interval '1 day' ) as date)
 when extract('dow' from cast('{start_date}' as date))=2 then cast((cast('{start_date}' as date) - interval '2 day') as date)
 when extract('dow' from cast('{start_date}' as date))=3 then cast((cast('{start_date}' as date) - interval '3 day') as date)
 when extract('dow' from cast('{start_date}' as date))=4 then cast((cast('{start_date}' as date) - interval '4 day') as date)
 when extract('dow' from cast('{start_date}' as date))=5 then cast((cast('{start_date}' as date) - interval '5 day') as date)
 when extract('dow' from cast('{start_date}' as date))=6 then cast((cast('{start_date}' as date) - interval '6 day') as date)
else cast('{start_date}' as date)
end as week_dt,

sum(coalesce("DP1",0)+coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))  over (partition by mb."SongID",
mb."C_Let")  as spins_total,
sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0)+coalesce("DP5",0))   over (partition by mb."SongID",
mb."C_Let") as spins_non_on,
sum(coalesce("DP2",0)+coalesce("DP3",0)+coalesce("DP4",0))   over (partition by mb."SongID",
mb."C_Let") as spins_am_pm_dr,

sum("MarketSpinsToDate") over (partition by mb."SongID", mb."C_Let")  as market_spins,
sum("SpinsToDate") over (partition by mb."SongID", mb."C_Let")  as station_spins,
sum("SpinsToDate") over (partition by mb."SongID") as format_spins,
sum("SpinsToDate") over (partition by artist_name) as format_artist_spins,
sum("SpinsToDate") over (partition by "C_Let",artist_name) as station_artist_spins,
sum("MarketSpinsToDate") over (partition by "C_Let",artist_name) as market_artist_spins
--,dense_rank() over (partition by mb."SongID",market_name  order by "SpinsToDate")
--null as station_market_rank

from dbo."MediabaseSongSpins" mb

join data.stations_v st
on st.call_letters = mb."C_Let"
join data.songs_v ds
on mb."SongID" = ds.mediabase_id


where
lower(format_code)='{format_code}' and
"StartDate"<='{start_date}' ) a


) mb

join adds_temp.demo_station_song_subset_{train_score}_{format_code}  sss
on mb."C_Let" = sss.cmm_station_calls
and mb."SongID" = sss.mediabase_id

where (mb.week_dt - song_release_date )>=0

);;





ALTER TABLE adds_temp.demo_mb_{format_code}  ADD PRIMARY KEY ( "SongID", "C_Let",week_dt);


-- CMM / Research
create table adds_temp.demo_cm_{format_code} as
(
select b.*,

max(max_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt  ) as max_pop_prior_unv,
min(min_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt  ) as min_pop_prior_unv,
sum(count_pop_prior*mean_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt )/nullif(sum(count_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt ),0) as mean_pop_prior_unv,
sum(count_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt ) as count_pop_prior_unv,
last_value(mr_pop_prior) over (partition by mediabase_id, breakout_id order by week_dt  ) as mr_pop_prior_unv,
max(mr_pop_prior_date) over (partition by mediabase_id, breakout_id order by week_dt  ) as mr_pop_prior_unv_date,


max(max_pop_artist_prior) over (partition by artist_name, breakout_id order by week_dt  ) as max_pop_artist_prior_unv,
min(min_pop_artist_prior) over (partition by artist_name, breakout_id order by week_dt  ) as min_pop_artist_prior_unv,
sum(count_pop_artist_prior*mean_pop_artist_prior) over (partition by artist_name, breakout_id order by week_dt )/nullif(sum(count_pop_artist_prior) over (partition by artist_name, breakout_id order by week_dt ),0) as mean_pop_artist_prior_unv,
sum(count_pop_artist_prior) over (partition by mediabase_id, breakout_id order by week_dt ) as count_pop_artist_prior_unv,
last_value(mr_pop_artist_prior) over (partition by artist_name, breakout_id order by week_dt  ) as mr_pop_artist_prior_unv,
max(mr_pop_artist_prior_date) over (partition by artist_name, breakout_id order by week_dt  ) as mr_pop_artist_prior_unv_date

from (

select a.*,

max(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as max_pop_prior,
min(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as min_pop_prior,
avg(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as mean_pop_prior,
stddev(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as std_pop_prior,
median(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as med_pop_prior,
max(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt rows between 0 preceding and 0 preceding ) as mr_pop_prior,
max(a.week_dt) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt rows between 0 preceding and 0 preceding ) as mr_pop_prior_date,
count(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as count_pop_prior,

max(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt ) as max_pop_artist_prior,
min(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt ) as min_pop_artist_prior,
avg(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt ) as mean_pop_artist_prior,
stddev(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt ) as std_pop_artist_prior,
count(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt ) as count_pop_artist_prior,
median(pop_all) over (partition by mediabase_id, cmm_station_calls, breakout_id order by a.week_dt ) as median_pop_prior,

max(pop_all) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt rows between 0 preceding and 0 preceding ) as mr_pop_artist_prior,
max(a.week_dt) over (partition by artist_name, cmm_station_calls, breakout_id order by a.week_dt rows between 0 preceding and 0 preceding ) as mr_pop_artist_prior_date


from
(
Select a1.*
from
(
select sss.cmm_station_calls, ds.mediabase_id, ds.artist_name, cmm.breakout_id, cmm.breakout_name,

case when extract('dow' from test_date)=1 then cast((test_date - interval '8 day' ) as date)
 when extract('dow' from test_date)=2 then cast((test_date - interval '9 day') as date)
 when extract('dow' from test_date)=3 then cast((test_date- interval '10 day') as date)
 when extract('dow' from test_date)=4 then cast((test_date - interval '11 day') as date)
 when extract('dow' from test_date)=5 then cast((test_date - interval '12 day') as date)
 when extract('dow' from test_date)=6 then cast((test_date - interval '13 day') as date)
else cast((test_date - interval '7 day') as date)
end as week_dt,


sum(breakout_respondents ) as total_respondents,
sum(case when project_type='Omt' then breakout_respondents else 0 end) as omt_respondents,
sum(case when project_type='Callout' then breakout_respondents else 0 end) as callout_respondents,
sum(case when project_type in ('Callout','Omt') then pop*breakout_respondents else null end)/sum(case when project_type in ('Callout','Omt') then breakout_respondents else null end ) as pop_all,
sum(case when project_type='Omt' then pop*breakout_respondents else 0 end)/sum(case when project_type='Omt' then breakout_respondents else null end) as pop_omt,
sum(case when project_type='Callout' then pop*breakout_respondents else 0 end)/sum(case when project_type='Callout' then breakout_respondents else null end) as pop_co,

avg(case when project_type='Custom Consolidated OMT'  then pop else 0 end) as pop_cc_omt,
avg(case when project_type='Custom Consolidated'  then pop else 0 end) as pop_cc_co,


sum(case when market_spins <0 then 0 else market_spins end ) as market_spins_cmm,
sum(case when station_spins <0 then 0 else station_spins end ) as station_spins_cmm


from
data.cmm cmm

join data.songs_v ds
on cmm.song_id = ds.song_id

join adds_temp.demo_station_song_subset_train_{format_code}  sss
on cmm.cmm_station_calls = sss.cmm_station_calls
and ds.mediabase_id = sss.mediabase_id


where test_date >= '{start_date}'

and breakout_respondents>0 -- this filters out any custom consolidated call-out/OMT
and (first_spin_thresh_week <= (date_trunc('week', cmm.test_date)::date- '8 day'::interval)::date  or ("GRC"='G' or "GRC" isnull) )
and (cmm.breakout_name IN ('*Core*', '*Old*', '*Young*',
    'Total', 'White', 'WAO', 'Hispanic', 'AA', 'AA/Hispanic', 'TOTAL(M)', 'TOTAL(F)', 'Asian')
        OR cmm.breakout_name LIKE 'F (%')
group by 1,2,3,4,5,6

UNION

select sss.cmm_station_calls, ds.mediabase_id, ds.artist_name,
CASE WHEN cmm.breakout_name='*Core*' then -1 when cmm.breakout_name='F (18-24)' then -2 else null end as breakout_id,
CASE WHEN cmm.breakout_name='*Core*' then 'Non-Core' when cmm.breakout_name='F (18-24)' then 'F (Other)' else null end as breakout_name,
case when extract('dow' from cmm.test_date)=1 then cast((cmm.test_date - interval '8 day' ) as date)
 when extract('dow' from cmm.test_date)=2 then cast((cmm.test_date - interval '9 day') as date)
 when extract('dow' from cmm.test_date)=3 then cast((cmm.test_date- interval '10 day') as date)
 when extract('dow' from cmm.test_date)=4 then cast((cmm.test_date - interval '11 day') as date)
 when extract('dow' from cmm.test_date)=5 then cast((cmm.test_date - interval '12 day') as date)
 when extract('dow' from cmm.test_date)=6 then cast((cmm.test_date - interval '13 day') as date)
else cast((cmm.test_date - interval '7 day') as date)
end as week_dt,


sum(cmm1.breakout_respondents - cmm.breakout_respondents ) as total_respondents,
sum(case when cmm.project_type='Omt' then cmm1.breakout_respondents - cmm.breakout_respondents else 0 end) as omt_respondents,
sum(case when cmm.project_type='Callout' then cmm1.breakout_respondents - cmm.breakout_respondents else 0 end) as callout_respondents,
sum(case when cmm.project_type in ('Callout','Omt') then (cmm1.pop*cmm1.breakout_respondents - cmm.pop*cmm.breakout_respondents) else null end)/sum(case when cmm.project_type in ('Callout','Omt') then NULLIF(cmm1.breakout_respondents - cmm.breakout_respondents,0) else null end ) as pop_all,
sum(case when cmm.project_type='Omt' then (cmm1.pop*cmm1.breakout_respondents - cmm.pop*cmm.breakout_respondents) else 0 end)/sum(case when cmm.project_type='Omt' then NULLIF(cmm1.breakout_respondents - cmm.breakout_respondents,0) else null end) as pop_omt,
sum(case when cmm.project_type='Callout' then (cmm1.pop*cmm1.breakout_respondents - cmm.pop*cmm.breakout_respondents) else 0 end)/sum(case when cmm.project_type='Callout' then NULLIF(cmm1.breakout_respondents - cmm.breakout_respondents,0) else null end) as pop_co,

avg(case when cmm.project_type='Custom Consolidated OMT'  then (cmm1.pop+ cmm.pop)/2 else 0 end) as pop_cc_omt,
avg(case when cmm.project_type='Custom Consolidated'  then (cmm1.pop+ cmm.pop)/2 else 0 end) as pop_cc_co,


sum(case when cmm.market_spins <0 then 0 else cmm.market_spins end ) as market_spins_cmm,
sum(case when cmm.station_spins <0 then 0 else cmm.station_spins end ) as station_spins_cmm


from
data.cmm cmm

join data.cmm cmm1
on cmm1.song_id = cmm.song_id
and cmm1.station_id = cmm.station_id
and cmm1.test_id = cmm.test_id
and cmm.project_type=cmm1.project_type

join data.songs_v ds
on cmm.song_id = ds.song_id

join adds_temp.demo_station_song_subset_train_{format_code}  sss
on cmm.cmm_station_calls = sss.cmm_station_calls
and ds.mediabase_id = sss.mediabase_id


where cmm.test_date >= '{start_date}'

and cmm.breakout_respondents>0 -- this filters out any custom consolidated call-out/OMT
and (first_spin_thresh_week <= (date_trunc('week', cmm.test_date)::date- '8 day'::interval)::date  or ("GRC"='G' or "GRC" isnull) )
and cmm.breakout_name IN ('*Core*', 'F (18-24)')
and cmm1.breakout_name = 'Total'
and cmm1.breakout_respondents*cmm1.pop > cmm.breakout_respondents*cmm.pop
--and (ds.mediabase_id = 1179699 or ds.artist_name = 'BRITNEY SPEARS')
--and cmm.station_id = 3322235
--and station_spins_non_overnight>{spins_filter}
group by 1,2,3,4,5,6) a1

where a1.pop_all < 150
) a

)  as b

)
;;

ALTER TABLE adds_temp.demo_cm_{format_code}  ADD PRIMARY KEY ( cmm_station_calls, mediabase_id, breakout_id, week_dt);;

/*
-- Adjusted GCR logic (implemented early 2021)
create table adds_temp.demo_gcr_{format_code} as
    (select a.mediabase_id,
        case when extract('dow' from week_dt)=1 then (week_dt - interval '1 day')::date
        when extract('dow' from week_dt)=2 then (week_dt - interval '2 day')::date
        when extract('dow' from week_dt)=3 then (week_dt - interval '3 day')::date
        when extract('dow' from week_dt)=4 then (week_dt - interval '4 day')::date
        when extract('dow' from week_dt)=5 then (week_dt - interval '5 day')::date
        when extract('dow' from week_dt)=6 then (week_dt - interval '6 day')::date
        else week_dt::date
        end as week_dt,
        gcr, gcr_adj,
        case when gcr='C' then 0 when gcr='R' then 1 when gcr='G' then 2 else null end as gcr_num


    from
        (select ds.mediabase_id,
            cast("EffectiveDate" as date)  as week_dt,
            "GRC" as gcr,
            case when "GRC"='C' then 'C'
            when "GRC" = 'R' then 'C'
            when "GRC" = 'G' then 'R' end as gcr_adj
        from
        dbo."Grcxfr" g
        join data.songs_v ds
            on trim(g."SongCode")= trim(ds.song_code)

        where "EffectiveDate"<>'G' and lower("FormatCode") = '{format_code}'

        union all

        select ds.mediabase_id,
            case
            when "GRC" = 'R' then cast("EffectiveDate" as date) +interval '{c_to_r}'
            when "GRC" = 'G' then cast("EffectiveDate" as date) +interval '{r_to_g}'
            end as week_dt,
            "GRC" as gcr,
            "GRC" as gcr_adj

        from
        dbo."Grcxfr" g
        join data.songs_v ds
            on trim(g."SongCode")= trim(ds.song_code)
        where "EffectiveDate"<>'G' and lower("FormatCode") = '{format_code}' and "GRC" in ('G', 'R')

        ) a
    join adds_temp.demo_song_subset_train_{format_code}  sss
        on a.mediabase_id = sss.mediabase_id

)

ALTER TABLE adds_temp.demo_gcr_{format_code}   ADD PRIMARY KEY (mediabase_id,week_dt)
)
 */


-- temp demo table for rr features
create table adds_temp.demo_rr_temp_{format_code} as
(select
mb.mediabase_id,
mb.station_id,
mb."C_Let",
mb.week_dt,
mb.artist_id,
mb."FirstLast",
mb."SongTitle",
mb."Market_Name",
mb.song_release_date,
brkts.breakout_id,
brkts.breakout_name,
cm.total_respondents,
cm.omt_respondents,
cm.callout_respondents,

rsafp.gcr  as gcr,
COALESCE(rsafp.gcr, rsafp.gcr_adj)  as gcr_adj,

cardinality(regexp_split_to_array(regexp_replace(upper(mb."FirstLast"), '[^[A-Z0-9\/\s]', '','g') , '(F\/.*)|(W\/.*)|[\/]')) as artist_count,
cardinality(regexp_split_to_array(upper(mb."FirstLast"), '(F\/.\S*)|(W\/.\S*)')) as feat_artist,
cardinality(regexp_split_to_array(upper(mb."SongTitle"), '(F\/.\S*)|(W\/.\S*)')) as feat_artist_song,
spins_total,
spins_non_on,
spins_am_pm_dr,

song_univ_spins,
artist_univ_spins,

market_spins,
station_spins,
format_spins,
format_artist_spins,
station_artist_spins,
market_artist_spins,

cm.pop_all,
cm.pop_co,
cm.pop_omt,

rsafp.taa_quintile,

case when omt_respondents=total_respondents then 'OMT_only'
when callout_respondents=total_respondents then 'CO_only'
when omt_respondents>0 and callout_respondents>0 then 'OMT_CO'
else null end omt_co_flag,




max_pop_prior,
min_pop_prior,
mean_pop_prior,
std_pop_prior,
med_pop_prior,
mr_pop_prior,
mr_pop_prior_date,
max(max_pop_prior_unv) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt ) as max_pop_prior_unv,
max(min_pop_prior_unv) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt ) as min_pop_prior_unv,
max(mean_pop_prior_unv) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt  ) as mean_pop_prior_unv,
max(count_pop_prior_unv) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt   ) as count_pop_prior_unv,
max(mr_pop_prior_unv) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt  ) as mr_pop_prior_unv,
max(mr_pop_prior_unv_date) over (partition by coalesce(mb."SongID", mb.mediabase_id) , brkts.breakout_id , mb.week_dt  ) as mr_pop_prior_unv_date,


max_pop_artist_prior,
min_pop_artist_prior,
mean_pop_artist_prior,
std_pop_artist_prior,
count_pop_artist_prior,
median_pop_prior,

mr_pop_artist_prior,
mr_pop_artist_prior_date,


max(max_pop_artist_prior_unv) over (partition by mb.artist_id , brkts.breakout_id, mb.week_dt ) as max_pop_artist_prior_unv,
max(min_pop_artist_prior_unv) over (partition by mb.artist_id , brkts.breakout_id, mb.week_dt ) as min_pop_artist_prior_unv,
max(mean_pop_artist_prior_unv) over (partition by mb.artist_id , brkts.breakout_id, mb.week_dt ) as mean_pop_artist_prior_unv,
max(count_pop_artist_prior_unv) over (partition by mb.artist_id , brkts.breakout_id, mb.week_dt ) as count_pop_artist_prior_unv,
max(mr_pop_artist_prior_unv) over (partition by mb.artist_id , brkts.breakout_id, mb.week_dt ) as mr_pop_artist_prior_unv



 from

 adds_temp.demo_mb_{format_code}  mb

 --*****************************************************************************************************************************************
 left join
 (
 Select distinct cmm_station_calls, mediabase_id, breakout_id, breakout_name
 from adds_temp.demo_cm_{format_code} AS dch
 ) as brkts
 on mb.mediabase_id=brkts.mediabase_id
 and mb."C_Let"=brkts.cmm_station_calls


--*****************************************************************************************************************************************
left
 join


adds_temp.demo_cm_{format_code} cm


on cm.cmm_station_calls = mb."C_Let"
and cm.mediabase_id = mb."SongID"
and cm.week_dt = mb."week_dt"
and brkts.breakout_id=cm.breakout_id


--*****************************************************************************************************************************************
left join dbo.rr_scores_adds_from_prod AS rsafp
on rsafp.mediabase_id = mb.mediabase_id
and rsafp.station_id = mb.station_id
and rsafp.week_dt = mb.week_dt

--*****************************************************************************************************************************************
/*
--left
full outer
join adds_temp.demo_gcr_{format_code} gcr
on gcr.mediabase_id = mb."SongID"
and gcr."week_dt" = mb."week_dt"
*/
--****************************************************************************************************************************************
where mb.station_id is not null
--and mb.mediabase_id = 1179699
--and mb.station_id = 3322235
--and brkts.breakout_id=1
order by artist_id, mediabase_id,  station_id, breakout_id, week_dt
)
;;




-- create indexes
CREATE INDEX ON adds_temp.demo_rr_temp_{format_code}   (mediabase_id, station_id, breakout_id, week_dt );;
CREATE INDEX ON adds_temp.demo_rr_temp_{format_code}   (artist_id, station_id, breakout_id, week_dt);;
CREATE INDEX ON adds_temp.demo_rr_temp_{format_code}   (mediabase_id, breakout_id);;
CREATE INDEX ON adds_temp.demo_rr_temp_{format_code}   (artist_id, breakout_id);;
CREATE INDEX ON adds_temp.demo_rr_temp_{format_code}   ("Market_Name", breakout_id);;
-- create indexes

-- Delete rows with null station_ids
DELETE
from adds_temp.demo_rr_temp_{format_code} AS drth
where station_id is null
;;

-- final feature table
create table adds_temp.demo_rr_features_{format_code} as
(select
mediabase_id,
station_id,
week_dt,
artist_id,
'{format_code}' as format_code,
"FirstLast",
"SongTitle",
"Market_Name",
song_release_date,
breakout_id,
breakout_name,
total_respondents,
omt_respondents,
callout_respondents,
gcr,
gcr_adj,

artist_count,
feat_artist,
feat_artist_song,
spins_total,
spins_non_on,
spins_am_pm_dr,

song_univ_spins,
artist_univ_spins,

market_spins,
station_spins,
format_spins,
format_artist_spins,
station_artist_spins,
market_artist_spins,

pop_all,
pop_co,
pop_omt,
omt_co_flag,
taa_quintile,


--Song Age
(week_dt - song_release_date )/7.0 as song_age_weeks,

--First spin
(week_dt - min(week_dt)  filter(where spins_total>0) over (partition by mediabase_id, station_id, breakout_id))/7.0 as song_station_weeks_since_first_spins,
(week_dt -min(week_dt)  filter(where spins_total>0) over (partition by mediabase_id, "Market_Name", breakout_id))/7.0 as song_market_weeks_since_first_spins,
(week_dt -min(week_dt) filter(where spins_total>0) over (partition by artist_id, station_id, breakout_id))/7.0 as artist_station_weeks_since_first_spins,
(week_dt -min(week_dt) filter(where spins_total>0)  over (partition by mediabase_id, breakout_id))/7.0 as song_weeks_since_first_spins,
(week_dt -min(week_dt) filter(where spins_total>0)  over (partition by artist_id, breakout_id))/7.0 as artist_weeks_since_first_spins,

--Time to Test
(week_dt - max(week_dt) filter(where pop_all notnull) over (partition by mediabase_id, breakout_id order by week_dt ))/7.0 as song_last_test_any_weeks,
(week_dt - max(week_dt) filter(where pop_co notnull) over (partition by mediabase_id, breakout_id order by week_dt))/7.0 as song_last_test_co_weeks,
(week_dt - max(week_dt) filter(where pop_omt notnull) over (partition by mediabase_id, breakout_id order by week_dt))/7.0 as song_last_test_omt_weeks,

(week_dt - min(week_dt) filter(where pop_all notnull) over (partition by mediabase_id, breakout_id order by week_dt  ))/7.0 as song_first_test_any_weeks,
(week_dt - min(week_dt) filter(where pop_co notnull) over (partition by mediabase_id, breakout_id order by week_dt  ))/7.0 as song_first_test_co_weeks,
(week_dt - min(week_dt) filter(where pop_omt notnull) over (partition by mediabase_id, breakout_id order by week_dt  ))/7.0 as song_first_test_omt_weeks,


--Last Spun
(week_dt -max(week_dt) filter(where spins_total>0) over (partition by mediabase_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding))/7.0 as song_weeks_since_last_spins,
(week_dt -max(week_dt) filter(where spins_total>0) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding))/7.0 as song_station_weeks_since_last_spins,

--more two station tests
-- edited to exclude omts for currents,i.e., two callouts at two station for currents
case when ((max(station_id) filter(where case when gcr_adj = 'C' then pop_co else pop_all end notnull) over (partition by mediabase_id, breakout_id order by week_dt))
- (min(station_id) filter(where case when
gcr_adj = 'C' then pop_co else pop_all end notnull) over (partition by mediabase_id, breakout_id order by week_dt))) > 0 then 1 else 0 end as station_test_1_plus,
case when max(station_id) filter(where pop_all notnull) over (partition by mediabase_id, breakout_id order by week_dt) = station_id then 1 else 0 end as station_test_1_id,


--Previous Spins
max(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_spins_song_station_prior,
max(spins_non_on) over (partition by mediabase_id, "Market_Name", breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_spins_song_market_prior,
max(spins_non_on) over (partition by artist_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_spins_artist_station_prior,

max(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_song_univ_spins_prior,
max(artist_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_artist_univ_spins,
max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_market_artist_spins_prior,
max(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as mr_market_spins_spins,

--Spin Change
max(spins_non_on) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_spins_song_station_prior,

max(spins_non_on) over (partition by mediabase_id, "Market_Name" , breakout_id, week_dt  )-
max(spins_non_on) over (partition by mediabase_id, "Market_Name", breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_spins_song_market_prior,

max(spins_non_on) over (partition by artist_id, station_id, breakout_id, week_dt ) -
max(spins_non_on) over (partition by artist_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_spins_artist_station_prior,

max(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_song_univ_spins_prior,

max(artist_univ_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(artist_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_artist_univ_spins_prior,

max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_market_artist_spins_prior,

max(market_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ) as diff_market_spins_spins_prior,



--Percent Spin Change
(sum(spins_non_on) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
sum(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*sum(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0) as perc_diff_spins_song_station_prior,

(sum(spins_non_on) over (partition by mediabase_id, "Market_Name" , breakout_id, week_dt  )-
sum(spins_non_on) over (partition by mediabase_id, "Market_Name", breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*sum(spins_non_on) over (partition by mediabase_id, "Market_Name", breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0)
as perc_diff_spins_song_market_prior,

(sum(spins_non_on) over (partition by artist_id, station_id , breakout_id, week_dt ) -
sum(spins_non_on) over (partition by artist_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*sum(spins_non_on) over (partition by artist_id, breakout_id, station_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0)
as perc_diff_spins_artist_station_prior,


(max(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*max(song_univ_spins) over (partition by mediabase_id, breakout_id, station_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0) as per_diff_song_univ_spins_prior,

(max(artist_univ_spins) over (partition by mediabase_id, station_id, week_dt  ) -
max(artist_univ_spins) over (partition by mediabase_id, station_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*max(artist_univ_spins) over (partition by mediabase_id, station_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0) as per_diff_artist_univ_spins_prior,

(max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*max(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0)
 as per_diff_market_artist_spins_prior,

(max(market_spins) over (partition by mediabase_id, station_id, breakout_id, week_dt  ) -
max(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ))/
nullif(1.0*max(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN 1 preceding AND 1 preceding ),0)
 as per_diff_market_spins_spins_prior,



--Total Spins

sum(spins_total) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_spins_song_station_prior,
sum(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_spins_non_on_song_station_prior,
sum(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_song_univ_spins_prior,
sum(artist_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_artist_univ_spins_prior,
sum(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_market_spins_prior,
sum(station_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_station_artist_spins_prior,
sum(market_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ) as total_market_artist_spins_prior,


-- spins per week all time
(sum(spins_non_on) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))
/(nullif((week_dt - min(week_dt) over (partition by mediabase_id, station_id, breakout_id))/7.0,0)) as avg_spins_song_station_prior,

(sum(song_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))/
(nullif((week_dt - min(week_dt) over (partition by mediabase_id, breakout_id))/7.0,0))  as avg_song_univ_spins_prior,

(sum(artist_univ_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))/
(nullif((week_dt - min(week_dt) over (partition by artist_id, breakout_id))/7.0,0))  as avg_artist_univ_spins_prior,

(sum(market_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))/
(nullif((week_dt - min(week_dt) over (partition by mediabase_id, "Market_Name", breakout_id))/7.0,0))  as avg_market_spins_prior,

(sum(station_artist_spins) over (partition by mediabase_id, station_id, breakout_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))/
(nullif((week_dt - min(week_dt) over (partition by artist_id, station_id, breakout_id))/7.0,0))  as avg_station_artist_spins_prior,

(sum(market_artist_spins) over (partition by mediabase_id, station_id order by week_dt ROWS BETWEEN unbounded preceding AND 1 preceding ))/
(nullif((week_dt - min(week_dt) over (partition by artist_id, "Market_Name"))/7.0,0))  as avg_market_artist_spins_prior,


GapFill(max_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as max_pop_prior,
GapFill(min_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as min_pop_prior,
GapFill(mean_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as mean_pop_prior,
GapFill(std_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as std_pop_prior,
GapFill(med_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as med_pop_prior,
GapFill(mr_pop_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as mr_pop_prior,
GapFill(mr_pop_prior_date) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as mr_pop_prior_date,

GapFill(max_pop_prior_unv) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as max_pop_prior_unv,
GapFill(min_pop_prior_unv) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as min_pop_prior_unv,
GapFill(mean_pop_prior_unv) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as mean_pop_prior_unv,
GapFill(count_pop_prior_unv) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as count_pop_prior_unv,
GapFill(mr_pop_prior_unv) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as mr_pop_prior_unv,
GapFill(mr_pop_prior_unv_date) over (partition by mediabase_id, station_id, breakout_id   order by week_dt rows between unbounded preceding and 1 preceding  ) as mr_pop_prior_unv_date,

GapFill(max_pop_artist_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as max_pop_artist_prior,
GapFill(min_pop_artist_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as min_pop_artist_prior,
GapFill(mean_pop_artist_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as mean_pop_artist_prior,
GapFill(std_pop_artist_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as std_pop_artist_prior,
GapFill(count_pop_artist_prior) over (partition by mediabase_id, station_id, breakout_id  order by week_dt rows between unbounded preceding and 1 preceding  ) as count_pop_artist_prior,
GapFill(max_pop_artist_prior_unv) over (partition by mediabase_id, station_id, breakout_id order by week_dt rows between unbounded preceding and 1 preceding  ) as max_pop_artist_prior_unv,
GapFill(min_pop_artist_prior_unv) over (partition by mediabase_id, station_id, breakout_id order by week_dt rows between unbounded preceding and 1 preceding  ) as min_pop_artist_prior_unv,
GapFill(mean_pop_artist_prior_unv) over (partition by mediabase_id, station_id, breakout_id order by week_dt rows between unbounded preceding and 1 preceding  ) as mean_pop_artist_prior_unv,
GapFill(count_pop_artist_prior_unv) over (partition by mediabase_id, station_id, breakout_id order by week_dt rows between unbounded preceding and 1 preceding  ) as count_pop_artist_prior_unv,
GapFill(mr_pop_artist_prior_unv) over (partition by mediabase_id, station_id, breakout_id order by week_dt rows between unbounded preceding and 1 preceding  ) as mr_pop_artist_prior_unv

 from
 adds_temp.demo_rr_temp_{format_code}


);;


CREATE INDEX ON adds_temp.demo_rr_temp_{format_code} (mediabase_id, station_id, breakout_id, week_dt )

/*
--Ignor universal pop statistics when only one callout has been performed
UPDATE adds_temp.demo_rr_features_{format_code}
SET mean_pop_prior_unv=NULL,
    max_pop_prior_unv=NULL,
    min_pop_prior_unv=NULL,
    mr_pop_prior_unv=NULL
WHERE count_pop_prior_unv=1
 */