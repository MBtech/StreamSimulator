import random
from routing import Routing

class Spout:
	def __init__(self, grouping='shuffle', parallelism=1, paraNext=1):
		self.parallelism = parallelism
		self.paraNext = paraNext
  		self.processed = parallelism*[0]
		self.grouping = grouping
                r = Routing(type=self.grouping)
                self.routingMethod = r.getRoutingMethod()

	#User needs to override this function in the derived class
        def execute(self):
                return "Hello is it me you are looking for",0

        def run(self):
		#Dummy message
                retmessages = list()
                key,value = self.execute()
                self.processed[0] +=1
                k = self.routingMethod(key, self.paraNext)
                retmessages.append((k,[key,value]))
                return retmessages

	def getProcessed(self):
		print self.processed
		return self.processed
