CREATE database if not exists capstone ;
use capstone;
-- 1.	“users" table
-- Create table query:
create table users(id VARCHAR(50),
age INT,
gender VARCHAR(50),
internet_usage VARCHAR(50),
income_bucket VARCHAR(50),
user_agent_string VARCHAR(50),
device_type VARCHAR(50),
websites VARCHAR(50),
movies VARCHAR(50),
music VARCHAR(50),
program  VARCHAR(50),
books VARCHAR(50),
negatives VARCHAR(50),
positives VARCHAR(50))
;
-- <Write the queries here>
-- 2.	“ads” table
-- <Write the queries here>
-- Create table query:


create table ads (text VARCHAR(200),
category VARCHAR(100),
keywords VARCHAR(100),
campaign_id VARCHAR(1600),
action varchar(300),
status VARCHAR(20),
target_gender VARCHAR(6),
target_age_start INT,
target_age_end INT,
target_city VARCHAR(50),
target_state VARCHAR(50),
target_country VARCHAR(50),
target_income_bucket VARCHAR(1600),
target_device VARCHAR(100),
cpc DOUBLE,
cpa DOUBLE,
cpm DOUBLE,
budget DOUBLE,
current_slot_budget DOUBLE,
date_range_start VARCHAR(100),
date_range_end VARCHAR(100),
time_range_start VARCHAR(100),
time_range_end VARCHAR(100))
;
-- 3.	“served_ads” table
-- Write the queries here>
-- Create table query:


create table served_ads (request_id VARCHAR(1600),
campaign_id VARCHAR(1600),
user_id VARCHAR(1600),
auction_cpm DOUBLE,
auction_cpc DOUBLE,
auction_cpa DOUBLE,
target_age_range VARCHAR(10),
target_location VARCHAR(50),
target_gender VARCHAR(5),
target_income_bucket VARCHAR(1600),
target_device_type VARCHAR(50),
campaign_start_ime VARCHAR(20),
campaign_end_time VARCHAR(20),
timestamp TIMESTAMP)
;
-- 4.	Query to load the users data

-- <Write the query here>
-- set global local_infile = 1;
LOAD data local infile '~/users_500k.csv' INTO TABLE table_name fields terminated BY ',' ignore 1 rows;


SET GLOBAL local_infile = 'ON';
set global local_infile = 1;
show global variables like 'local_infile';
SHOW VARIABLES LIKE "secure_file_priv";

LOAD data infile 'D:\Learning_Material\online advertising platform capstone project\users_500k.csv' INTO TABLE users;
  LOAD DATA INFILE 'D:\Learning_Material\online advertising platform capstone project\users_500k.csv' INTO TABLE users;
show databases;
use sys;

select * from sys_config;
use mysql;
use performance_schema;
show tables;
use information_schema;
show tables;