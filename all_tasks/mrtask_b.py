from mrjob.job import MRJob
from mrjob.step import MRStep

# 2.	Which pickup location generates the most revenue?

INDEX_PULocationID = 7
INDEX_total_amount = 16

class MapReduce_Task_B(MRJob):

	
	def steps(self):
		return [
			MRStep(mapper=self.mapper, reducer=self.reducer),
			MRStep(reducer=self.reducer_maxRevenue)
		]
	
	def mapper(_self, _, line):
		# Input value is each record line
		
		# Skip header line
		if not line.startswith('VendorID'):
			data = line.strip().split(',')
			
			# Output Key is PULocationID and value is revenue, each record indicate a trip
			yield(data[INDEX_PULocationID], float(data[INDEX_total_amount]) )
		
	def reducer(_self, PULocationID, revenue):
		# Input Key is PULocationID and value is revenue, each record indicate a trip
		
		# Output is None, and value is tuple having two value - Total Revenue and PULocationID
		yield(None, (sum(revenue), PULocationID))
	
	def reducer_maxRevenue(_self, _, tuple_value):
		# Finding the VendorID and Revenue with maxiumum Revenue
		(PULocationID_Revenue, PULocationID) = max(tuple_value)
		
		# Output Key is PULocationID and value is Revenue having maximum Revenue
		yield(PULocationID, PULocationID_Revenue)

if __name__ == '__main__':
    MapReduce_Task_B.run()
