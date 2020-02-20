from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import desc, asc
from itertools import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession


def getSameStockCodeRecords(dataframe, columnName, itemCode):
    """ returns the [InvoiceNo and columnName] of items that their 'stockCode' is the same as 'itemCode'

    :param dataframe: whole structured dataframe of products information
    :param columnName: column that we want to compare, in this data it is 'stockCode'
    :param itemCode: value of stockCode that we are looking for
    """
    return dataframe.select('InvoiceNo', columnName)\
                 .where((dataframe['stockCode'] == itemCode))
    

def getIntersectRecords(firstRecords, secondRecords):
    return firstRecords.select('InvoiceNo')\
                            .intersect(secondRecords.select('InvoiceNo'))

def getDistinctVals(dataframe, itemsetLevel):
    allVal = []
    for i in range(0, itemsetLevel):
        row = dataframe.select(dataframe[i]).collect()
        for j in range(0, len(row)):
            allVal.append(row[j][0])

    return spark.createDataFrame(allVal, StringType()).distinct().collect()  

def validateComb(previousItemset, distinctOne, allPreviousItemsets):
    if (distinctOne[0] == previousItemset[1]) or (distinctOne[0] == previousItemset[0]):
        return False
    
    trueItemsets = []
    for i in range(0, len(allPreviousItemsets)):
        trueItemsets.append([allPreviousItemsets[i][0],allPreviousItemsets[i][1]])
    preItemsetList = []
    for j in range(0, len(previousItemset)):
        preItemsetList.append(previousItemset[j])
  

    for i in range(1,len(preItemsetList)):
        tempItemset = preItemsetList
        tempItemset[i] = distinctOne[0]
        if tempItemset in trueItemsets:
            return True
        tempItemset.reverse()
        if tempItemset in trueItemsets:
            return True
        else:
            return False
    return True

def getValidItemsets(previousComb, combNum):
    possibleCombs = []
    distincts = getDistinctVals(previousComb, combNum)
    allCombinations = previousComb.drop('support').collect()
    
    for i in allCombinations:
        for j in distincts:
            if validateComb(i, j, allCombinations):
                newItemset = []
                for k in range(len(i)):
                    newItemset.append(i[k])
                newItemset.append(j[0])
                possibleCombs.append(newItemset)
    return possibleCombs
                

def twoItemset(dataframe, oneItems, treshhold):
    supportVals = []
    possibleCombinations = combinations(oneItems.collect(), 2)

    for i in possibleCombinations:
        
        firstItem = i[0]['stockCode']
        secondItem = i[1]['stockCode']
        firstProductRecords = (dataframe.select('InvoiceNo', 'stockCode')
                     .where((dataframe['stockCode'] == firstItem)))

        secondProductRecords = (dataframe.select('InvoiceNo', 'stockCode')
                     .where((dataframe['stockCode'] == secondItem)))
        sameBoughtProducts = firstProductRecords.select('InvoiceNo').intersect(secondProductRecords.select('InvoiceNo'))
        supportVals.append((firstItem, secondItem, sameBoughtProducts.count()))



    columns = ['product1', 'product2', 'support']
    twoItemsCounts = spark.createDataFrame(supportVals, columns)
    treshholdPrim = treshhold * m
    twoItems = twoItemsCounts.filter(twoItemsCounts['support'] > treshholdPrim)
    return twoItems


def oneItemSet(dataframe, colName, treshhold):
    m = numOfDistinctItems(dataframe, "InvoiceNo")
    itemsCount = dataframe.groupBy("stockCode").count()
    print(itemsCount)
    items = itemsCount.select(itemsCount['stockCode'], itemsCount['count'] / m)
    return items.select(items['stockCode']).filter("`(count / " + str(m) + ")` > " + str(treshhold))

def localFrequentApriori(n, dataframe):
    m = numOfDistinctItems(dataframe, "InvoiceNo")
    eachPartitionNum = int(m / 5) - 1
    datasets = []
    for k in range(0, 5):
        newDF = dataframe.select(dataframe['InvoiceNo'], dataframe['stockCode'])\
        .filter("`InvoiceNo` > " + str(k*eachPartitionNum+536365))\
        .filter("`InvoiceNo` < " + str((k+1)*eachPartitionNum+536365))
        datasets.append(newDF)
        
        for q in range(0,5):
            apriori(datasets[q], 0.1, 0.05)

    
def getProductRecords(df,  counterItemsets, item):
    productRecordsList = []
    for p in range(0, counterItemsets + 1):
        productRecordsList.append(getSameStockCodeRecords(df, 'stockCode', item[p]))
    return productRecordsList
    

def otherItemsets(preItemset, treshhold):
    counterItemsets = 2
    items = getValidItemsets(preItemset, counterItemsets)
    if not items:
        return 0
    supportVals = []
    counter = 0
    
    for i in items:
        productRecordsList =  getProductRecords(dataframe, counterItemsets, i )
    
        sameBoughtProducts = productRecordsList[0]   
        for k in range(0, counterItemsets):
            sameBoughtProducts = getIntersectRecords(sameBoughtProducts, productRecordsList[k])
        supportValue = (sameBoughtProducts.count()) / m
        
        products = []
        for r in range(0,counterItemsets + 1):
            productCode = productRecordsList[r].collect()[0]['stockCode']
            products.append(productCode)
        products.append(supportValue)

        supportVals.append(products) 

    columns = []
    for t in range(0, counterItemsets + 1):
        columns.append('product' + str(t+1))
    columns.append('support')
    #columns = ['product1', 'product2','product3', 'support']
    threeItemsCounts = spark.createDataFrame(supportVals, columns)
    treshholdPrim = treshhold * m
    threeItems = threeItemsCounts.filter(threeItemsCounts['support'] > treshholdPrim) 
    return threeItems


def numOfDistinctItems( dataframe, Column ):
    return dataframe.select(Column).distinct().count()

def _input( fileName, contextSession ):
    return contextSession.read.csv( fileName, header=True)

def apriori(dataframe, supTreshhold, confTreshhold):
    oneItems = oneItemSet(dataframe, 'stockCode', 0.06)
    print('One items collect:' + str(oneItems.collect()))
    twoItems = twoItemset(dataframe, oneItems, 0.05)
    print('two items collect:' + str(twoItems.collect()))
    threeItemset = otherItemsets(twoItems , 0.05)
    while threeItemset:
        print('more than 3' + str(threeItemset.collect()))
        threeItemset = otherItemsets(threeItemset, 0.05)



if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark SQL Apriori Implementation") \
        .master("local[*]")\
        .getOrCreate()
    dataframe = _input("Online Retail.csv", spark)
    m = numOfDistinctItems(dataframe, "InvoiceNo")
    apriori(dataframe, 0.07, 0.05)
    
    
    