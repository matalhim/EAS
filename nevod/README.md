# DBs Structure

## NEAS_DB
- `YYYY_MM_DD_e`
- `YYYY_MM_DD_e_events`  

## NEVOD_DB
- `YYYY_MM_DD`  


## RESULT
- `YYYY_MM_DD_e_TW_time_ns`  
    Results for YYYY_MM_DD_e in time window time from `YYYY_MM_DD` and `YYYY_MM_DD_e_events`  
- `RUN_{run}_DATE_YYYY_MM_DD_TW_1000_ns`  
  YYYY_MM_DD_e_TW_time_ns split by {run}.
- `RUN_{run}_DATE_YYYY_MM_DD_events`  
  {run} events from YYYY_MM_DD
- `RUN_{run}_events`  
  {run} events
- `RUN_{run}_not_events`  
  {run} non-events
