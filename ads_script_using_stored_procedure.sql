drop PROCEDURE if exists ads_procedure;
DELIMITER $$
CREATE PROCEDURE `ads_procedure` (
in text VARCHAR(200),
in category VARCHAR(100),
in keywords VARCHAR(100),
in campaign_id VARCHAR(1600),
in action VARCHAR(160),
in target_gender VARCHAR(6),
in target_age_start INT,
in target_age_end INT,
in target_city VARCHAR(50),
in target_state VARCHAR(50),
in target_country VARCHAR(50),
in target_income_bucket VARCHAR(1600),
in target_device VARCHAR(100),
in cpc DOUBLE,
in cpa DOUBLE,
in budget DOUBLE,
in date_range_start VARCHAR(100),
in date_range_end VARCHAR(100),
in time_range_start VARCHAR(100),
in time_range_end VARCHAR(100))

BEGIN
IF LOWER(action) = 'new campaign' THEN
        SET @status = 'active';
    ELSEIF LOWER(action)= 'update campaign' THEN
        SET @status = 'active';  
    ELSE 
        SET @status = 'inactive' ;
    END IF;
SET @current_slot_budget = (cast(time_range_end as time)-cast(time_range_start as time));
SET date_range_end= (cast(date_range_end as date)-cast(date_range_end as date));
SET date_range_start= (cast(date_range_start as date)-cast(date_range_start as date));
SET @cpm = (0.0075*cpa+0.0005*cpa);
INSERT INTO ads values(text ,category ,keywords ,campaign_id,action,@status ,target_gender ,target_age_start ,target_age_end ,target_city ,target_state ,target_country ,target_income_bucket ,target_device ,cpc ,cpa ,@cpm ,budget ,@current_slot_budget ,date_range_start ,date_range_end ,time_range_start ,time_range_end );
END$$

-- CALL ads_procedure ('abc' ,'abc' ,'keywords' ,'campaign_id','new Campaign','m' ,5 ,10 ,'target_city' ,'target_state' ,'target_country' ,'target_income_bucket' ,'target_device' ,45 ,25 ,7777  ,"2018-06-15" ,"2017-06-15" ,'19:03:10.000001' ,'19:30:10.000001');
/*alter table ads
drop column action;*/
select * from ads;


