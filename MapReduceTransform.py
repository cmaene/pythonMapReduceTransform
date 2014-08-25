import sys, json, csv, pandas, numpy

"""
Trasnform data with Python MapReduce Framework
 - preprocess will create an intermediate txt.file, 'dx.json'
usage: python2.7 MapReduceTransform.py input.txt > output.txt

"""
class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        for line in data:
            record = json.loads(line)
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        #for item in self.result:    # use this to check the output
            #print jenc.encode(item)
	return(self.result)

mr = MapReduce()

# =============================
# preprocess will format and create an inputdata, 'dx.json'
'''
preprocess input examples:
 - prepared in R:  write.table(newvec, file="newvec081814.csv", sep=",", row.names=FALSE, col.names=FALSE, quote=FALSE)
2635x6,328190xxx,402xx892,M,8x,0,724,780,427,805,807,426,780,272,788,244,600,1
478x20,320211xxx,38xxx520,F,x9,0,780,410,486,255,710,492,427,458,733,NA,NA,2
478x20,3320416xxx,41xx9441,F,x0,0,780,038,785,507,410,518,255,263,112,995,494,3
'''
def preprocess():
    file =  open('dx.json',"w") # process each line and save
    #f = open(sys.argv[1],'r')
    for i in range(6,17):
        j = []
	f = open(sys.argv[1],'r')
        for line in f:
	    j = line.split(",")
            dic = {}  
	    dic = j[17].rstrip('\n')
	    dic +="-"
	    dic += j[i]
	    file.write(json.dumps(dic)+"\n") # writes each dictionary..
        file.close
'''
mapper inputdata example:
"1-724"
"2-780"
"3-780"
'''
def mapper(record):
    # key: combination of ID and DX code
    key   = record
    mr.emit_intermediate(key, 1)

'''
reducer output examples:
["4-507", 1]
["2-NA", 2]
["1-427", 1]
'''
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# ref:
# http://stackoverflow.com/questions/14745022/pandas-dataframe-how-do-i-split-a-column-into-two 
# http://stackoverflow.com/questions/17116814/pandas-how-do-i-split-text-in-a-column-into-multiple-columns
# http://stackoverflow.com/questions/22798934/pandas-long-to-wide-reshape
# http://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column
def pandatransform(data):
    df = pandas.DataFrame(data, columns=['idx','freq'])
    newdf = pandas.DataFrame(df.idx.str.split('-',1).tolist(), columns=['id','dx'])
    df = df.join(newdf)
    df['truefalse']=numpy.where(df['dx']=='NA', 'F', 'T')
    #print(df) # to check result
    df['order']='dx'+df.dx.astype(str)
    #dxassociation = df.pivot(index='id', columns='order', values='freq')
    dxassociation = df.pivot(index='id', columns='order', values='truefalse')
    #dxassociation=dxassociation.drop('dxNA',1)
    #print(dxassociation) # to check result
    dxassociation.to_csv('dxassociation.csv')

# =============================
if __name__ == '__main__':
  preprocess()
  inputdata = open('dx.json')
  result = mr.execute(inputdata, mapper, reducer)
  pandatransform(result)
