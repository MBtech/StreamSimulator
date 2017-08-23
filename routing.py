import random

class Routing:
	def __init__(self, type='shuffle'):
		print "Initialization"
		self.type = type

	#def decideInstance(self, message):
        #        if self.grouping=='shuffle':
        #                k = random.choice(range(0, self.paraNext))
        #        elif self.grouping == 'key':
        #                k = hash(message[1]) % self.paraNext
        #        return k

	def shuffleRouting(self,  message,paraNext):
		return random.choice(range(0, paraNext))
	
	def keyRouting(self, message, paraNext):
		return hash(message) % paraNext

	def getRoutingMethod(self):
		if self.type == 'shuffle':
			return self.shuffleRouting
		if self.type=='key':
			return self.keyRouting
		
