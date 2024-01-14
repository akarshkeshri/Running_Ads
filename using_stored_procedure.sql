DELIMITER $$
CREATE PROCEDURE `SelectAllCustomers` ()
BEGIN
SELECT name FROM employee;
END$$

CALL SelectAllCustomers ();

use test;
DELIMITER $$
CREATE PROCEDURE `Customers` (in action varchar(20) ,out status varchar(20))
BEGIN
IF LOWER(action) = 'new campaign' THEN
        SET status = 'active';
    ELSEIF LOWER(action)= 'update campaign' THEN
        SET status = 'active';  
    ELSE 
        SET status = 'inactive' ;
    END IF;

END$$

CALL Customers ('new campaign',@status);
select * from employee;
INSERT INTO employee values('hammy',130000,120000/12,'Update Campaign', CALL Customers ('new campaign',@status));
INSERT INTO employee values('hammy',130000,120000/12,'Update Campaign', @status);
drop PROCEDURE if exists learn1;
DELIMITER $$
CREATE PROCEDURE `learn1` (in name varchar(20), in annual_salary double,in  monthly_salary double, in action varchar(20), in status varchar(20))

BEGIN
IF LOWER(action) = 'new campaign' THEN
        SET status = 'active';
    ELSEIF LOWER(action)= 'update campaign' THEN
        SET status = 'active';  
    ELSE 
        SET status = 'inactive' ;
    END IF;
INSERT INTO employee values(name,annual_salary,monthly_salary/12,action, status);

END$$

CALL learn1 ('pammy',150000,120000/12,'Update Campaign', '');
select * from employee;
INSERT INTO employee values('hammy',130000,120000/12,'Update Campaign', @status);


