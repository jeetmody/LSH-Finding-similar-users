from pyspark import SparkContext
from collections import defaultdict
from itertools import combinations
from operator import add
import time
import sys
#sc = SparkContext.getOrCreate()
sc = SparkContext(appName="inf553")
#Start code 
def hash20(x):
	band = [[y] for y in xrange(5)]
	ret = []
	min = 101
	for i in range(20):
		minV = 101
		for j in x[1]:
			res = (3*int(j)+13*i)%100
			if(res <minV):
				minV = res
		#print int(i/4)-1
		band[int(i/4)].append(minV)
	ret = {}
	for i in band:
		ret[tuple(i)]=x[0]
	ans = []
	retList=[]
	for k,v in ret.iteritems():
		retList.append([tuple(k),x[0]])
	return (retList)
	

def func2(x):

	retList=[]
	if(len(x[1])>1):
		y = list(combinations(x[1],2))

		for i in y:
			retList.append(tuple(sorted(list(i))))
		return retList
	else:
		return []

def func3(x):
	ret = set()
	for j in list(x[1]):
		for i in j:
			if(tuple(sorted([str(x[0]),str(i)])) not in ret):
				ret.add(tuple(sorted([str(x[0]),str(i)])))

	print list(ret),"\n\n"
	return list(ret)

def func4(x,inputDict):
	pairs = []
	temp1 = int(str(x[0][1:]))
	temp2 = int(str(x[1][1:]))
	
	pair1 = [temp1,temp2]
	pair2 = [temp2,temp1]
	movie1 = set(inputDict[temp1])
	movie2 = set(inputDict[temp2])
	uni = movie1.union(movie2)
	inter = movie1.intersection(movie2)
	jacc = len(inter)/(len(uni)*1.0)
	pairs.append([pair1[0],[pair1[1],jacc]])
	pairs.append([pair2[0],[pair2[1],jacc]])
	return pairs
    



def func5(x):
	similarUsers= list(x[1])
	res1 = sorted(similarUsers,key=lambda x: x[0])
	res2 = sorted(res1,key=lambda x: x[1], reverse = True)
	
	if(5>len(res2)):
		siz = len(res2)
	else:
		siz = 5
	finalRes = res2[:siz]
	finalResSorted = sorted(finalRes, key = lambda x: x[0])
	#print res2
	ans=[]

	for i in range(5):
		if((i==len(res2))):
			break
		ans.append(finalResSorted[i][0])
	ret=[x[0],ans]
	return ret
def main():
	start = time.time()
	inputFile = sys.argv[1]
	rdd = sc.textFile(inputFile).map(lambda x:x.split(','))
	inp = rdd.collect()
	inputDict = {}#change input dict before 
	for i in inp:
		inputDict[int(str(i[0][1:]))]=i[1:]
	
	templ=[]

	for k,v in inputDict.iteritems():
		templ=[]
		for i in v:
			templ.append(int(i))
		inputDict[k]=templ
	rdd2 = rdd.map(lambda x:(x[0],x[1:])).flatMap(hash20).groupByKey().flatMap(func2).distinct().flatMap(lambda x:func4(x,inputDict)).groupByKey().map(func5)
	ans = rdd2.collect()
	outputFile = sys.argv[2]
	file = open(outputFile,'w')
	for i in sorted(ans):
		if(len(i[1])==0):
			continue
		op = "U"+str(i[0])+":"
		file.write("%s"%op)
		for j in range(len(i[1])):
			if (j != len(i[1])-1):
				op = "U"+str(i[1][j])+","
				file.write("%s"%op)
			else:
				op = "U"+str(i[1][j])
				file.write("%s"%op)
		file.write("\n")
if __name__ == '__main__':
    main()
