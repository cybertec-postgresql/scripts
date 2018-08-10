/* 
  Sample queries to analyze pg_stat_statements snapshots (for example gathered with the stat_activity_monitoring.py)
*/
select avg(snap_time - xact_start) as average_transaction_duration from sas;

select avg(snap_time - backend_start) as average_session_duration from sas;

select max(snap_time - xact_start) as max_xact_duration from sas;

select round(avg(count), 1) as average_active_queries from (select count(*) from sas where state != 'idle' group by snap_time) a;

select
  snap_time as most_active_snapshot_times,
  count(*)
from
  sas
where
  state != 'idle'
group by
  snap_time
order by

select
  *,
  round(count::numeric / sum(count) over() * 100, 1) as pct,
  sum(count) over() as total_entries
from (
  select
    state,
    count(*)
  from
    sas
  group by
    state
) a
order by
  count desc;


select
  *,
  round(locked_entries / total_entries::numeric * 100, 1) as pct_locked
from (
  select
    count(*) as total_entries,
    count(*) FILTER (where wait_event_type ~* 'lock') as locked_entries
  from
    sas 
) a;

select
  query as top_queries,
  count(*)
from
  sas
where
  state != 'idle'
group by
  query
order by
  count(*) desc
limit
  10;


select
  snap_time as most_active_snapshot_times,
  count(*)
from
  sas
where
  state != 'idle'
group by
  snap_time
order by
  count(*) desc
limit
  10;
