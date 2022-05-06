create database if not exists airline_ontime_parquet;
use airline_ontime_parquet;

create table if not exists airports (
  iata string,
  airport string,
  city string,
  state double,
  country string,
  lat double,
  lon double
)
stored as parquet
tblproperties ("transactional"="true", "transactional_properties"="insert_only");

load data inpath '${datapath}/airline_ontime_parquet.db/airports' into table airports;

create table if not exists airlines (
  code string,
  description string
)
stored as parquet
tblproperties ("transactional"="true", "transactional_properties"="insert_only");

load data inpath '${datapath}/airline_ontime_parquet.db/airlines' into table airlines;

create table if not exists planes (
  tailnum string,
  owner_type string,
  manufacturer string,
  issue_date string,
  model string,
  status string,
  aircraft_type string,
  engine_type string,
  year int
)
stored as parquet
tblproperties ("transactional"="true", "transactional_properties"="insert_only");

load data inpath '${datapath}/airline_ontime_parquet.db/planes' into table planes;

create table if not exists flights (
  month int,
  dayofmonth int,
  dayofweek int,
  deptime  int,
  crsdeptime int,
  arrtime int,
  crsarrtime int,
  uniquecarrier string,
  flightnum int,
  tailnum string,
  actualelapsedtime int,
  crselapsedtime int,
  airtime int,
  arrdelay int,
  depdelay int,
  origin string,
  dest string,
  distance int,
  taxiin int,
  taxiout int,
  cancelled int,
  cancellationcode string,
  diverted string,
  carrierdelay int,
  weatherdelay int,
  nasdelay int,
  securitydelay int,
  lateaircraftdelay int
)
partitioned by (year int)
stored as parquet
tblproperties ("transactional"="true", "transactional_properties"="insert_only");

load data inpath '${datapath}/airline_ontime_parquet.db/flights/year=2004' into table flights partition (year=2004);
load data inpath '${datapath}/airline_ontime_parquet.db/flights/year=2005' into table flights partition (year=2005);
load data inpath '${datapath}/airline_ontime_parquet.db/flights/year=2006' into table flights partition (year=2006);
load data inpath '${datapath}/airline_ontime_parquet.db/flights/year=2007' into table flights partition (year=2007);
load data inpath '${datapath}/airline_ontime_parquet.db/flights/year=2008' into table flights partition (year=2008);

ALTER TABLE planes ADD CONSTRAINT planes_pq_pk PRIMARY KEY (tailnum) DISABLE NOVALIDATE;
ALTER TABLE flights ADD CONSTRAINT planes_pq_fk  FOREIGN KEY (tailnum) REFERENCES planes(tailnum) DISABLE NOVALIDATE RELY;

ALTER TABLE airlines ADD CONSTRAINT airlines_pq_pk PRIMARY KEY (code) DISABLE NOVALIDATE;
ALTER TABLE flights ADD CONSTRAINT airlines_pq_fk FOREIGN KEY (uniquecarrier) REFERENCES airlines(code) DISABLE NOVALIDATE RELY;

ALTER TABLE airports ADD CONSTRAINT airports_pq_pk PRIMARY KEY (iata) DISABLE NOVALIDATE;
ALTER TABLE flights ADD CONSTRAINT airports_pq_orig_fk FOREIGN KEY (origin) REFERENCES airports(iata) DISABLE NOVALIDATE RELY;
ALTER TABLE flights ADD CONSTRAINT airports_pq_dest_fk FOREIGN KEY (dest) REFERENCES airports(iata) DISABLE NOVALIDATE RELY;
