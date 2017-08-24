from streamSim import Spout
from streamSim import Sink
from streamSim import Bolt
from streamSim import SplitBolt
from random import randint


class iotSpout(Spout):
	def execute(self):
        	return [randint(0,1000), randint(30,60)]

class iotTokenizer(Bolt):
	def execute(self, message):
		return message[0],message[1]

class iotDecider(SplitBolt):
	def execute(self, message):
		if message[1]>50:
			return message[0],message[1],1
		else:
			return message[0],message[1],0

class iotEmergency(Sink):
	def execute(self, message):
		print "Emergency"

class iotPattern(SplitBolt):
	def execute(self, message):
		if message[1]<40:
			return message[0],message[1],1
		else:
			return message[0],message[1],0


class iotStorage(Sink):
	def execute(self, message):
		print "Storage the data"
	
class iotNotify(Sink):
	def execute(self, message):
		print "Call the doctor"


S1 = iotSpout(paraNext=1)
S2 = iotTokenizer()
S3 = iotDecider(paraNext=[1,1])
S4 = iotPattern(paraNext=[1,1])
S5 = iotEmergency()
S6 = iotStorage()
S7 = iotNotify()

for i in range(0,10):
	ret1 = S3.run(S2.run(S1.run()))
	ret2 = S4.run(ret1[0])
	S5.run(ret1[1])
	S6.run(ret2[0])
	S7.run(ret2[1])
	

S5.getProcessed()
