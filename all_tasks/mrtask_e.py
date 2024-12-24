from mrjob.job import MRJob
from mrjob.step import MRStep

# 5.	Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

INDEX_PULocationID = 7
INDEX_tip_amount = 13
INDEX_total_amount = 16

class MapReduce_Task_E(MRJob):

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
			
			# Output Key is PULocationID and value is tuple having two value - tip amount, and total trip amount
			yield( data[INDEX_PULocationID], (float(data[INDEX_tip_amount]), float(data[INDEX_total_amount])) )
	
	def reducer(_self, PULocationID, tuple_value):
		# Input Key is PULocationID and value is tuple having two value - tip amount, and total trip amount
		sum_tip_amount, sum_total_amount = (sum(x) for x in zip(*tuple_value))
		
		# Output is None, and value is tuple having two value - Total Revenue and PULocationID
		yield(None, (sum_tip_amount/sum_total_amount, PULocationID ))
	
	def reducer_sorting(_self, _, tuple_value):
		# Sorting the records according the ratio of tip to total revenue from higher to lower
		# and Output Key is PULocationID and value is ratio of tip to total revenue
		for ratio_tiprevenue, PULocationID in sorted(tuple_value, reverse=True):
			yield(PULocationID, ratio_tiprevenue)
 
if __name__ == '__main__':
	MapReduce_Task_E.run()
