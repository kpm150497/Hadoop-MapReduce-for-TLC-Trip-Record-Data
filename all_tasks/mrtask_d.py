from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
 
# 4.	What is the average trip time for different pickup locations?

INDEX_PULocationID = 7
INDEX_tpep_pickup_datetime = 1
INDEX_tpep_dropoff_datetime = 2

class MapReduce_Task_D(MRJob):
 
	def steps(self):
		return [
			MRStep(mapper=self.mapper, reducer=self.reducer),
			MRStep(reducer=self.reducer_sorting)
		]
	
	def mapper(_self, _, line):
		# Input value is each record line
		
		# Skip header line
		if not line.startswith('VendorID'):
			data = line.strip().split(',')

			pickuptime = datetime.strptime(data[INDEX_tpep_pickup_datetime], '%Y-%m-%d %H:%M:%S')
			dropofftime = datetime.strptime(data[INDEX_tpep_dropoff_datetime], '%Y-%m-%d %H:%M:%S')
			triptime_minutes = round( (dropofftime-pickuptime).total_seconds() / 60.0, 2)

			# Output Key is PULocationID and value is tuple having two value - triptime in minutes, and trip count i.e. 1
			yield( data[INDEX_PULocationID], (triptime_minutes, 1) )
 		
	def reducer(_self, PULocationID, tuple_value):
		# Input Key is PULocationID and value is tuple having two value - triptime in minutes, and trip count
		sum_triptime_minutes, sum_trip_count = (sum(x) for x in zip(*tuple_value))
		
		# Output is None, and value is tuple having two value - Total Revenue and PULocationID
		yield(None, (sum_triptime_minutes/sum_trip_count, PULocationID ))
	
	def reducer_sorting(_self, _, tuple_value):
		# Sorting the records according the average trip time from higher to lower
		# and Output Key is PULocationID and value is average trip time
		for avgtriptime, PULocationID in sorted(tuple_value, reverse=True):
			yield(PULocationID, avgtriptime)
 
if __name__ == '__main__':
     MapReduce_Task_D.run()
