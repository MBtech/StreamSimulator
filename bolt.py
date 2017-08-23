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
        	print "Do something"


	def execute(self, message):
		print "Process Messages"
		return message[1]

	def run(self, messages):
		retmessages = list()
                for message in messages:
                        self.processed[message[0]] +=1
                        value = self.execute(message)
			k = self.routingMethod(value,self.paraNext)
                        retmessages.append((k,value))
		return retmessages

	def getProcessed(self):
		print self.processed
		return self.processed
