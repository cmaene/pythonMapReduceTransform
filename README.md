pythonMapReduceTransform
========================

data transformation with MapReduce and Pandas - the output is the data for association rules analysis.
I have a code to do the same in R (see "createdata4associationrules.md") but the code took a few days to complete (N=43048 with up to 11 different DX code per obs.) 

Processing data using MapReduce is much better/faster - the script finished in less than 15 seconds! Pandas is also an excellent collection of data manipulation tools..


INPUT*****************************

 - prepared in R:  write.table(newvec, file="newvec081814.csv", sep=",", row.names=FALSE, col.names=FALSE, quote=FALSE)
2635x6,328190xxx,402xx892,M,8x,0,724,780,427,805,807,426,780,272,788,244,600,1
478x20,320211xxx,38xxx520,F,x9,0,780,410,486,255,710,492,427,458,733,NA,NA,2
478x20,3320416xxx,41xx9441,F,x0,0,780,038,785,507,410,518,255,263,112,995,494,3

OUTPUT*****************************

order dx008 dx038 dx112 dx244 dx255 dx263 dx272 dx276 dx285 dx287  ...  dxV58  \
id                                                                 ...          
1       NaN   NaN   NaN     T   NaN   NaN     T   NaN   NaN   NaN  ...    NaN   
2       NaN   NaN   NaN   NaN     T   NaN   NaN   NaN   NaN   NaN  ...    NaN   
3       NaN     T     T   NaN     T     T   NaN   NaN   NaN   NaN  ...    NaN   
4         T   NaN   NaN   NaN     T   NaN   NaN     T   NaN   NaN  ...      T   
5       NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     T     T  ...    NaN
