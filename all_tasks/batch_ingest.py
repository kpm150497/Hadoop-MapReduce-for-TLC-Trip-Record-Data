import happybase
import re
import sys
from datetime import datetime

# To track Row Count. Also, this is used to skip first record
row_number = 0

def batch_ingest_tripdata(dns_emr, filename):
	
	global row_number
	
	# Connect to RDS instance (user need to pass the DNS of RDS instance.
	print("Connection to HBase...", dns_emr)
	con = happybase.Connection(dns_emr)
	
	# Opeing the connection
	con.open()

	# Table name is fixed as yellow_tripdata_full n HBase.
	htable = con.table('yellow_tripdata_full')
	hbatch = htable.batch(batch_size = 10000)
	
	# Column Indexes
	INDEX_VendorID = 0
	INDEX_tpep_pickup_datetime = 1
	INDEX_tpep_dropoff_datetime = 2
	INDEX_passenger_count = 3
	INDEX_trip_distance = 4
	INDEX_RatecodeID = 5
	INDEX_store_and_fwd_flag = 6
	INDEX_PULocationID = 7
	INDEX_DOLocationID = 8
	INDEX_payment_type = 9
	INDEX_fare_amount = 10
	INDEX_extra = 11
	INDEX_mta_tax = 12
	INDEX_tip_amount = 13
	INDEX_tolls_amount = 14
	INDEX_improvement_surcharge = 15
	INDEX_total_amount = 16
	INDEX_congestion_surcharge = 17
	INDEX_airport_fee = 18

	# Getting YEAR and MONTH from filename for Key-Prfix
	key_prefix_filename = ''
	x = re.findall("[0-9]{4}-[0-9]{2}", filename)
	if (x):
		key_prefix_filename = 'CSV-' + x[0] + '-'

	start_time = datetime.now()
	print("Opening the file...")
	with open(filename) as fp:
		for line in fp:
			if row_number%1000000 == 0:
				print(datetime.now(), ': Rows inserted :', row_number)

			line = line.strip()
			vals = line.split(",")

			if vals[0] != 'VendorID': # Skip first record (header) of file
				row_number += 1
				################ Logic for Generating ROW-KEY using 'Salting Approach' with some specific rowkey patterns - Start ################
				# Getting the UNIQUE ROW-KEY for Record
				# If desired Key-Prefix (YEAR and MONTH) is not available in filename, then extract it from pickup-time
				unique_key = key_prefix_filename
				if key_prefix_filename == '':
					# Extract Year and Month from pick-up time
					YEAR_MONTH = vals[INDEX_tpep_pickup_datetime][:8]

					# Created the key-prfix as "SOURCE" + "YEAR" + "MONTH" + "ROW_NUMBER"
					unique_key = 'CSV-' + YEAR_MONTH
				unique_key = unique_key + format(row_number, '010d')
				################ Logic for Generating ROW-KEY using 'Salting Approach' with some specific rowkey patterns - End   ################

				dictRow = {}
				dictRow[b'trips:VendorID']					= bytes(vals[INDEX_VendorID], encoding='utf8')
				dictRow[b'trips:tpep_pickup_datetime']		= bytes(vals[INDEX_tpep_pickup_datetime], encoding='utf8')
				dictRow[b'trips:tpep_dropoff_datetime']		= bytes(vals[INDEX_tpep_dropoff_datetime], encoding='utf8')
				dictRow[b'trips:passenger_count']			= bytes(vals[INDEX_passenger_count], encoding='utf8')
				dictRow[b'trips:trip_distance']				= bytes(vals[INDEX_trip_distance], encoding='utf8')
				dictRow[b'trips:RatecodeID']				= bytes(vals[INDEX_RatecodeID], encoding='utf8')
				dictRow[b'trips:store_and_fwd_flag']		= bytes(vals[INDEX_store_and_fwd_flag], encoding='utf8')
				dictRow[b'trips:PULocationID']				= bytes(vals[INDEX_PULocationID], encoding='utf8')
				dictRow[b'trips:DOLocationID']				= bytes(vals[INDEX_DOLocationID], encoding='utf8')
				dictRow[b'trips:payment_type']				= bytes(vals[INDEX_payment_type], encoding='utf8')
				dictRow[b'trips:fare_amount']				= bytes(vals[INDEX_fare_amount], encoding='utf8')
				dictRow[b'trips:extra']						= bytes(vals[INDEX_extra], encoding='utf8')
				dictRow[b'trips:mta_tax']					= bytes(vals[INDEX_mta_tax], encoding='utf8')
				dictRow[b'trips:tip_amount']				= bytes(vals[INDEX_tip_amount], encoding='utf8')
				dictRow[b'trips:tolls_amount']				= bytes(vals[INDEX_tolls_amount], encoding='utf8')
				dictRow[b'trips:improvement_surcharge']		= bytes(vals[INDEX_improvement_surcharge], encoding='utf8')
				dictRow[b'trips:total_amount']				= bytes(vals[INDEX_total_amount], encoding='utf8')
				dictRow[b'trips:congestion_surcharge']		= bytes(vals[INDEX_congestion_surcharge], encoding='utf8')
				dictRow[b'trips:airport_fee']				= bytes(vals[INDEX_airport_fee], encoding='utf8')

				hbatch.put(bytes(unique_key, encoding='utf8'), dictRow)
	
	# Send any leftover rows
	hbatch.send()
	
	print("Close Connection...")
	con.close()
	
	diff = datetime.now() - start_time
	print("File Name: ", filename)
	print("Total Rows inserted: ", row_number)
	print("Time taken to insert ", diff)

# Actual execution starts here
# Example of function call
#   batch_ingest_tripdata("ec2-54-167-39-92.compute-1.amazonaws.com", "/root/tripdata/yellow_tripdata_2017-03.csv")
#   batch_ingest_tripdata("ec2-54-167-39-92.compute-1.amazonaws.com", "/root/tripdata/yellow_tripdata_2017-04.csv")

# Example of Python code execution command
#   Python batch_ingest.py $DNS_EMR /root/tripdata/yellow_tripdata_2017-03.csv /root/tripdata/yellow_tripdata_2017-04.csv

if len(sys.argv) < 3:
	print("Please enter following arguments.\n 1. DNS (URL) for RDS (MySQL) Instance.\n 2. Valid filenames with full path to load to HBase (any numbers)")
else:
	for i in range(2, len(sys.argv)):
		batch_ingest_tripdata(sys.argv[1], sys.argv[i])
	print("All files loaded successfully.")

