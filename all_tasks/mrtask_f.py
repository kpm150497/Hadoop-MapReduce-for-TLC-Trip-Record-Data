from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

# 6.	How does revenue vary over time? Calculate the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

INDEX_tpep_pickup_datetime = 1
INDEX_total_amount = 16

class MapReduce_Task_F(MRJob):

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
			
			# Extract month, hour, and weekday/weekend from pickuptime
			pickuptime = datetime.strptime(data[INDEX_tpep_pickup_datetime], '%Y-%m-%d %H:%M:%S')
			mnth = pickuptime.month # strftime("%m")
			hour = pickuptime.hour # strftime("%H")
			day = pickuptime.date().weekday()
			weekday = 'Weekday'
			if day == 5 or day == 6:
				weekday = 'Weekend'
			
			# Combining the fields to suit mapreduce
			comparableKey = format(mnth, '02d') + '-' + weekday + '-' + format(hour, '02d')
			
			# Output Key is key combination of Month-Weekday/Weekend-Hour and value is tuple having two value - revenue, and trip count i.e. 1
			yield( comparableKey, (float(data[INDEX_total_amount]), 1) )
		
	def reducer(_self, key, tuple_value):
		# Input Key is key combination of Month-Weekday/Weekend-Hour and value is tuple having two value - revenue, and trip count
		sum_trip_revenue, sum_trip_count = (sum(x) for x in zip(*tuple_value))
		
		# Output is None, and value is tuple having two value - Total Revenue and PULocationID
		yield(None, (key, sum_trip_revenue/sum_trip_count ))
	
	def reducer_sorting(_self, _, tuple_value):
		# Sorting the records according the time from higher to lower
		# and Output Key is Month-Weekday/Weekend-Hour and value is average trip revenue
		for key, avgtriprevenue in sorted(tuple_value):
			yield(key, avgtriprevenue)
 
if __name__ == '__main__':
	MapReduce_Task_F.run()
