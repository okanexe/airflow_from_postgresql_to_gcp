Data Engineer Assessment

1 - Build an online RDBMS-database (technology depends on you)<br/>
	•	DB name must be de_assessment.<br/>
	•	Min. 5 tables and every table must include 50 rows of data.<br/>
	•	DB type must be retail business (supermarket, automotive sales, or online retail sales).<br/>
	•	FK, Constraints, and Normalization rules should be considered<br/>
	•	id and created_at are mandatory columns that each table must contain.<br/>


2 - ETL with Apache-Airflow (technology in your local system)<br/>
	•	Build an ETL, which converts every data in db_assessment to a CSV file and then transfers it to GCP Bucket on a daily basis. *** <br/>
	•	The bucket name must be de_assessment_bucket.<br/>
	•	ETL process must be Incremental, e.g. yesterday we had 50 data and we transferred these data to buckets, today we have 10 more new data. And the ETL process must transfer these data to the bucket, not the whole data.<br/>
*** Table for everyday activity MUST be in a CSV file.<br/>


3 - ETL with GCP services (technology in your GCP account)<br/>
	•	We have a bucket that is called de_assessment_bucket. Now, we have to convert and transfer this data to Big Query to make an analysis.
	•	Every CSV file must be parsed and the parsed data must be added to the tables.<br/>
	•	BQ tables must at least include a wild_cards table and a partition table.<br/>
	•	ETL process must be Incremental. For example; yesterday we transferred 5 CSV files from the bucket to the BQ tables. Today we’ll transfer 2 more new CSV files. And the ETL process must transfer these CSV files to the tables, not whole CSV files.<br/>
