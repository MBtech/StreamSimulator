import random
from routing import Routing

class SplitBolt:
	def __init__(self, grouping='shuffle', parallelism=1, paraNext=[1]):
		self.parallelism = parallelism
		self.paraNext = paraNext
  		self.processed = parallelism*[0]
		self.grouping = grouping
		r = Routing(type=self.grouping)
                self.routingMethod = r.getRoutingMethod()

	def execute(self, message):
		""" Execute function runs the user logic and the derived classes need to override this method"""
		return message[0],message[1],random.choice(range(0, len(self.paraNext)))

	def run(self, messages):
		nStages = len(self.paraNext)
		retmessages = [None]*nStages
		for i in range(0, nStages):
			retmessages[i] = list()
                for message in messages:
                        k = 0
                        self.processed[message[0]] +=1
                        key,value,i = self.execute(message[1])

			k = self.routingMethod(key, self.paraNext[i])
                        retmessages[i].append((k,[key,value]))
		return retmessages

	def getProcessed(self):
		print self.processed
		return self.processed
