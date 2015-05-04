import json

MACHINES = {}

def addWorker(kind, address):
	if kind not in MACHINES: MACHINES[kind] = []
	MACHINES[kind].append(address)

def releaseWorker(kind, address):
	if kind in MACHINES:
		if address in MACHINES[kind]:
			MACHINES.remove(address)

def toJson(path):
	f = open(path, "w")
	f.write(json.dumps(MACHINES))
	f.close()
	print 'Machines\' info has been written to \"'+path + '\"'  




