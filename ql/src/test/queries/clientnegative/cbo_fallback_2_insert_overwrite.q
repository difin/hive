create table aa1 ( stf_id string);
create table bb1 ( stf_id string);
create table cc1 ( stf_id string);
create table ff1 ( x string);

explain
from ff1 as a join cc1 as b 
insert overwrite table aa1 select   stf_id GROUP BY b.stf_id
insert overwrite table bb1 select b.stf_id GROUP BY b.stf_id
;
