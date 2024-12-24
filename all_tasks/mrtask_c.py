from mrjob.job import MRJob
from mrjob.step import MRStep

# 3.	What are the different payment types used by customers and their count? The final results should be in a sorted format.

INDEX_payment_type = 9

class MapReduce_Task_C(MRJob):

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
			
			# Output Key is payment_type and value is 1 each record indicate a transaction
			yield(data[INDEX_payment_type], 1)
		
	def reducer(_self, payment_type, count):
		# Input Key is payment_type and value is 1 each record indicate a transaction
		
		# Output is None, and value is tuple having two value - Total Count and payment_type
		yield(None, (sum(count), payment_type))
	
	def reducer_sorting(_self, _, tuple_value):
		# Sorting the records according the number of transaction from higher to lower
		# and Output Key is payment_type and value is count for each payment_type
		for count, payment_type in sorted(tuple_value, reverse=True):
			yield(payment_type, count)

if __name__ == '__main__':
    MapReduce_Task_C.run()
