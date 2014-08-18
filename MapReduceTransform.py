import MapReduce, sys, json, csv

"""
Trasnform data with Python MapReduce Framework
"""
mr = MapReduce.MapReduce()

# =============================
# preprocess will format and create an inputdata, 'dx.json'
'''
preprocess input examples:
 - prepared in R:  write.table(newvec, file="newvec081814.csv", sep=",", row.names=FALSE, col.names=FALSE, quote=FALSE)
26356,328190772,40284892,M,89,0,724,780,427,805,807,426,780,272,788,244,600,1
47820,320211584,38389520,F,79,0,780,410,486,255,710,492,427,458,733,NA,NA,2
47820,332041698,41229441,F,80,0,780,038,785,507,410,518,255,263,112,995,494,3
47820,323651018,39204065,F,79,0,786,507,513,255,745,008,427,710,492,518,276,4
71869,310146287,35915170,M,77,0,578,562,287,567,285,311,300,733,NA,NA,NA,5
'''
def preprocess():
    file =  open('dx.json',"w") # process each line and save
    #f = open(sys.argv[1],'r')
    diclist = []
    for i in range(6,17):
        j = []
	f = open(sys.argv[1],'r')
        for line in f:
	    j = line.split(",")
            dic = {}
	    #if j[i]!="NA":    
	    dic = j[17].rstrip('\n')
	    dic +="-"
	    dic += j[i]
	    # dic["count"] = 1
	    file.write(json.dumps(dic)+"\n") # writes each dictionary..
	    #diclist.append(dic)
        file.close
'''
mapper inputdata example:
"1-724"
"2-780"
"3-780"
"4-786"
"5-578"
"1-780"
'''
def mapper(record):
    # key: document identifier
    # value: document contents
    key   = record
    mr.emit_intermediate(key, 1)

'''
reducer output examples:
["4-507", 1]
["2-NA", 2]
["1-427", 1]
["1-724", 1]
["5-NA", 3]
'''
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  preprocess()
  inputdata = open('dx.json')
  mr.execute(inputdata, mapper, reducer)
