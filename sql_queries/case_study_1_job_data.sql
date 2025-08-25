USE job_analysis;
SELECT ds, 
    COUNT(job_id) AS total_jobs,
    SUM(time_spent) / 3600 AS total_hours_spent, 
    COUNT(job_id) / (SUM(time_spent) / 3600) AS jobs_per_hour 
FROM job_data WHERE STR_TO_DATE(ds, '%m/%d/%Y') BETWEEN '2020-11-01' AND '2020-11-30' 
GROUP BY ds ORDER BY ds;

WITH DailyThroughput AS ( 
    SELECT
        ds, COUNT(event) AS events, SUM(time_spent) AS total_time, 
        COUNT(event) / SUM(time_spent) AS daily_throughput 
    FROM job_data GROUP BY ds 
)
SELECT
    ds, daily_throughput, 
    AVG(daily_throughput) OVER ( ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS rolling_avg_throughput
FROM DailyThroughput ORDER BY ds; 



SELECT
    language,
    (COUNT(job_id) * 100.0) / (SELECT COUNT(job_id) FROM job_data) AS percentage_share
FROM
    job_data GROUP BY language ORDER BY percentage_share DESC;
    
    

SELECT
    ds, job_id, actor_id, event, language, time_spent, org,
    COUNT(*) AS duplicate_count
FROM
    job_data
GROUP BY
	ds, job_id, actor_id,event,language,time_spent,org HAVING COUNT(*) > 1;
    
    
