import random

class Sink:
	def __init__(self, parallelism=1):
		self.parallelism = parallelism
  		self.processed = parallelism*[0]

	def execute(self, message):
		print "Do something with the received message"

	def run(self, messages):
		for message in messages:
			self.processed[message[0]] +=1
			self.execute(message[1])

	def getProcessed(self):
		print self.processed
		return self.processed
