
f1 = open('../Test_Cases/1k_Samples_Output/Part2/genes_score.txt', 'r')
f2 = open('result.txt', 'r')

set = {}
for line in f1:
	data = line.split(',')
	gene = data[0]
	set[gene] = data[1]

for line in f2:
	data = line.split(',')
	if (data[0] in set):
		if not (data[1][:-2] in set[data[0]]):
			print "Data does not match: %s, %s" %(data[1][:-2],set[data[0]])
	else:
		print "gene %s does not exist" %(data[0])
