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
	#print x
	band = [[y] for y in xrange(5)]

	#band[0]=[]
	#band[1]=[]
	#band[2]=[]
	#band[3]=[]
	#band[4]=[]
	

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

	#for i in band:
	#	i = tuple(i)
	#	print i
	#print x[0],":",band,"\n\n"
	ret = {}
	for i in band:
		ret[tuple(i)]=x[0]
		#ret.append(minV)
	#print (x[0],band)
	#print ret
	ans = []
	retList=[]
	for k,v in ret.iteritems():
		#print k,v
		#k2 = k[1:]
		#k2 = sorted(k2)
		#k3 = []
		#k3.append(k[0])
		#for i in k2:
		#	k3.append(i)
		#k3.append(k2)
		retList.append([tuple(k),x[0]])
	#print retList,"\n\n"
	return (retList)
	

def func2(x):

	result = {}
	#print x[0],"x[0]"
	#print list(x[1]),"x[1]"
	'''for i in x[1]:
		result[i]=[]
		for j in x[1]:
			if(j!=i):
				result[i].append(j)'''
	retList=[]
	#for k,v in result.iteritems():
	#	retList.append([k,v])
	#print retList,"----------------------------"
	#print len(retList[1]),retList
	if(len(x[1])>1):
		y = list(combinations(x[1],2))

		for i in y:
			retList.append(tuple(sorted(list(i))))

		#print retList,"\n\n\n"
	#if (len(retList)>1):
		return retList
	else:
		return []

def func3(x):
	#print x[0],list(x[1])
	#return []
	ret = set()
	for j in list(x[1]):
		for i in j:
			if(tuple(sorted([str(x[0]),str(i)])) not in ret):
				#print tuple(sorted([str(x[0]),str(i)])),"6666666666666666666666"
				ret.add(tuple(sorted([str(x[0]),str(i)])))

	print list(ret),"\n\n"
	return list(ret)

def func4(x,inputDict):
	#print x[0],list(x[1])
	#print inp
	#inputDict = {}#change input dict before 
	#for i in inp:
	#	inputDict[int(str(i[0][1:]))]=i[1:]
	#print inputDict, "##################"
	#templ=[]
	#for k,v in inputDict.iteritems():
	#	templ=[]
	#	for i in v:
	#		templ.append(int(i))
	#	inputDict[k]=templ
	#print x[0],"----",x[1]
	#return []
	pairs = []
	#print x,"xxxxx"
	#print inputDict
	#for i in list(x[1]):
		#print i
	#for i in x:
	#	print str(i[0][1:])
	temp1 = int(str(x[0][1:]))
	temp2 = int(str(x[1][1:]))
	
	pair1 = [temp1,temp2]
	pair2 = [temp2,temp1]
		#print pair1,"pair1"
		#print pair2,"pair2"
	movie1 = set(inputDict[temp1])
		#print movie1,"movie1"
		#print movie2
	movie2 = set(inputDict[temp2])
		#print movie2,"movie2"
	uni = movie1.union(movie2)
		#print uni
	inter = movie1.intersection(movie2)
		#inter = [filter(lambda x: x in movie1, sublist) for sublist in movie2]
		#print inter
	jacc = len(inter)/(len(uni)*1.0)
		#print jacc
	pairs.append([pair1[0],[pair1[1],jacc]])
	pairs.append([pair2[0],[pair2[1],jacc]])
	#print pairs,"***********************"
	        


 
	return pairs
    



def func5(x):
	#print x[0],list(x[1])
	similarUsers= list(x[1])
	res1 = sorted(similarUsers,key=lambda x: x[0])
	#print res1,"res1"
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
	#print ret,"\n\n"
	return ret


	
	#return []



def main():
	start = time.time()
	inputFile = sys.argv[1]
	#print "hello"

    #sup = float(sys.argv[2])
    #output = sys.argv[3]
	rdd = sc.textFile(inputFile).map(lambda x:x.split(','))
	inp = rdd.collect()
    #print rdd.collect()
	inputDict = {}#change input dict before 
	for i in inp:
		inputDict[int(str(i[0][1:]))]=i[1:]
	#print inputDict, "##################"
	templ=[]

	for k,v in inputDict.iteritems():
		templ=[]
		for i in v:
			templ.append(int(i))
		inputDict[k]=templ

	#print inputDict


	rdd2 = rdd.map(lambda x:(x[0],x[1:])).flatMap(hash20).groupByKey().flatMap(func2).distinct().flatMap(lambda x:func4(x,inputDict)).groupByKey().map(func5)
	#anstemp = rdd2.collect()
	#print anstemp,"temppppp"
	
	#.groupByKey().flatMap(func3).distinct().groupByKey().flatMap(lambda x:func4(x,inp)).groupByKey().map(func5)
	#rdd3 = rdd2.flatMap(lambda x:func4(x,inputDict)).groupByKey().map(func5)
	
	ans = rdd2.collect()
	#print ans
	#print "hello"
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
	#print time.time()-start,"))))))(((((("

    #print ans
    #for i in ans:
    #	print "hjh",i[0],list(i[1])
    





if __name__ == '__main__':
    main()
