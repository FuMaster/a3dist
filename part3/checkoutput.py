
f1 = open('../Test_Cases/1k_Samples_Output/Part3/samples_sim.txt', 'r')
f2 = open('result.txt', 'r')

set = {}
for line in f1:
	data = line.split(',')
	sample1 = data[0]
	sample2 = data[1]
	if sample1 > sample2:
		set[sample1 + "," + sample2] = data[2][:-2]
	else:
		set[sample2 + "," + sample1] = data[2][:-2]

print "%s_" %(set["sample_2,sample_1"])
for line in f2:
	data = line.split(',')
	sample1 = data[0]
	sample2 = data[1]
	if sample1 > sample2:
		if sample1+","+sample2 in set:
			num = set[sample1+"," + sample2]
			if not(num == data[2][:len(num)]):
				print "%s, %s: %s != %s" %(sample1,sample2,data[2][:len(num)],num)
		else:
			print "pair %s, %s does not exist" %(sample1,sample2)
	else:
		if sample2+","+sample1 in set:
			num = set[sample2+","+sample1]
			if not(num == data[2][:len(num)]):
				print "%s, %s: %s != %s" %(sample1,sample2,data[2][:len(num)],num)
		else:
			print "pair %s, %s does not exist" %(sample1,sample2)
