import random
from routing import Routing

class Bolt:
	def __init__(self, grouping='shuffle', parallelism=1, paraNext=1):
		self.parallelism = parallelism
		self.paraNext = paraNext
  		self.processed = parallelism*[0]
		self.grouping = grouping
		r = Routing(type=self.grouping)
		self.routingMethod = r.getRoutingMethod()


	def execute(self, message):
		return message[0],message[1]

	def run(self, messages):
		retmessages = list()
                for message in messages:
                        self.processed[message[0]] +=1
                        key,value = self.execute(message[1])
			k = self.routingMethod(key,self.paraNext)
                        retmessages.append((k,[key,value]))
		return retmessages

	def getProcessed(self):
		print self.processed
		return self.processed
