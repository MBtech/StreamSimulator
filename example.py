from bolt import Bolt
from sink import Sink
from spout import Spout
from splitBolt import SplitBolt

sp = Spout(paraNext=2)
blt = SplitBolt(parallelism=2,paraNext=[2,2], grouping='key')
#blt = Bolt(parallelism=2, paraNext=2)
si = Sink(parallelism=2)
si2 = Sink(parallelism=2)

for i in range(0,10):
	ret = blt.run(sp.run())
	si.run(ret[0])
	si2.run(ret[1])

si.getProcessed()
