import sys

from google.cloud import bigquery

if __name__=="__main__":
##	print(sys.argv[1], sys.argv[2])
##	exit(1)
	file_path = sys.argv[1] # input csv file
	try:
		assert file_path.endswith(".csv")
	except Exception as ae:
		print(f"Input file [{file_path}] is likely not a csv. Please provide csv file")
		exit(1)
	  
	table_id = sys.argv[2] # "your-project.your_dataset.your_table_name"
  
	# Construct a BigQuery client object.
	client = bigquery.Client(project=table_id.split(".")[0])
  
	job_config = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
	)
	
	with open(file_path, "rb") as source_file:
		job = client.load_table_from_file(source_file, table_id, job_config=job_config)
	
	job.result()  # Waits for the job to complete.
	
	table = client.get_table(table_id)	# Make an API request.
  
	print(
		"Loaded {} rows and {} columns to {}".format(
			table.num_rows, len(table.schema), table_id
		)
	)
