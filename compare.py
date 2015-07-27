f = open('results.txt','r')
contents = f.readlines()
f.close()

f = open('correct.txt', 'r')
contents1 = f.readlines()
f.close()

results = {}

for line in contents:
	id = line.split(',')[0]
	genes = ",".join(line.split(',')[1:])
	results[id] = genes

correct = {}

for line in contents1:
	id = line.split(',')[0]
	genes = ",".join(line.split(',')[1:])
	correct[id] = genes

for id in results:
	if not correct[id] == results[id]:
		print id,correct[id]
		print id,results[id]
		print "incorrect"
	
print "end"
