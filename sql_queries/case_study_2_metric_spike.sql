Use metric_spike;

Alter table users add column temp_occurred_at2 DATETIME;
Update users set temp_occurred_at2 =STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
Alter table users DROP column created_at;
Alter table users change column temp_occurred_at2 created_at DATETIME;

Create table events(
  user_id INT,
  occurred_at varchar(100),
  event_type varchar(50),
  event_name varchar(100),
  location varchar(50),
  device varchar(50),
  user_type INT
  );
  
  SHOW VARIABLES LIKE 'secure_file_priv';
  
  SET GLOBAL local_infile = 1;
  
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
  
Select *from events;
ALTER TABLE events
RENAME COLUMN occurred_at TO occured_at;

ALTER TABLE events
RENAME COLUMN occured_at TO occurred_at;

Alter table events add column temp_occurred_at DATETIME;
SET SQL_SAFE_UPDATES = 0;
Update events set temp_occurred_at =STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
Alter table events DROP column occurred_at;
Alter table events change column temp_occurred_at occurred_at DATETIME;

Create table email_events(
user_id INT,
occurred_at varchar(100),
action varchar(100),
user_type int);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

Alter table email_events add column temp_occurred_at1 DATETIME;
Update email_events set temp_occurred_at1 =STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
Alter table email_events DROP column occurred_at;
Alter table email_events change column temp_occurred_at1 occurred_at DATETIME;

Select *from email_events ;

SELECT
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM events
GROUP BY year, week_number
ORDER BY year, week_number;
    
    
SELECT
    YEAR(created_at) AS year,
    WEEK(created_at) AS week_number,
    COUNT(user_id) AS new_users
FROM users
    GROUP BY year, week_number
ORDER BY year,week_number;



WITH user_cohort AS (
    SELECT
        user_id,
        created_at AS signup_date
    FROM
        users
),
weekly_activity AS (
    SELECT
        user_id,
        occurred_at AS activity_date
    FROM
        events
)
SELECT
    YEAR(uc.signup_date) AS signup_year,
    WEEK(uc.signup_date) AS signup_week,
    FLOOR(DATEDIFF(wa.activity_date, uc.signup_date) / 7) AS weeks_since_signup,
    COUNT(DISTINCT wa.user_id) AS retained_users
FROM
    user_cohort uc
JOIN
    weekly_activity wa ON uc.user_id = wa.user_id
WHERE
    uc.signup_date IS NOT NULL AND wa.activity_date IS NOT NULL
GROUP BY
    signup_year,
    signup_week,
    weeks_since_signup
ORDER BY
    signup_year,
    signup_week,
    weeks_since_signup;
    

# Task 4
SELECT
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week_number,
    device,
    COUNT(DISTINCT user_id) AS weekly_active_users
FROM events
GROUP BY year, week_number, device
ORDER BY year, week_number, device;
    
#Task 5
SELECT
    YEAR(occurred_at) AS year,
    WEEK(occurred_at) AS week_number,
    action,
    COUNT(user_id) AS total_actions
FROM email_events
GROUP BY year, week_number, action
ORDER BY year, week_number, action;
