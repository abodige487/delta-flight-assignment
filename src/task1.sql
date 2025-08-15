with ranked as (
select *,
row_number() over(partition by flightkey order by lastupdt desc) as rnk
from flight_leg)
select flightkey,flightnum,flight_dt,orig_arpt,dest_arpt,flightstatus,lastupdt from ranked where rnk=1;
