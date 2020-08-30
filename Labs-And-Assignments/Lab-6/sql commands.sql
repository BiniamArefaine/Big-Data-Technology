CREATE EXTERNAL TABLE employee_datasets ( name STRING, job_title STRING, department STRING, full_or_part_time STRING, salary_hourly STRING, typicalHours FLOAT, annual_salary DOUBLE, hourly_rate FLOAT) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 

WITH SERDEPROPERTIES ("separatorChar" = ",","quoteChar" = "\"","escapeChar"	= "\\") 
LOCATION '/user/cloudera/cs523' 
TBLPROPERTIES ("skip.header.line.count"="1");

