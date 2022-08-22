import pyspark
import time

sc = pyspark.SparkContext()


def transactions_clean(line): #to filter transactions dataset
        try:
                fields = line.split(',')
                if len(fields)!= 7:
                        return False
                float(fields[5])
                float(fields[6])
                return True
        except:
                return False

def contracts_clean(line): #to filter contracts dataset
        try:
                fields = line.split(',')
                if len(fields) != 5:
                        return False
                float(fields[3])
                return True

        except:
                False

def blocks_clean(line): #to filter blocks dataset
        try:
                fields = line.split(',')
                if len(fields)!=9:
                        return False

                float(fields[0])
                float(fields[3])
                float(fields[7])
                return True

        except:
                return False


linestransact = sc.textFile('/data/ethereum/transactions')
linescontract = sc.textFile('/data/ethereum/contracts')
linesblock = sc.textFile('/data/ethereum/blocks')

cleanlinestransact = linestransact.filter(transactions_clean)
cleanlinescontract = linescontract.filter(contracts_clean)
cleanlinesblock = linesblock.filter(blocks_clean)

time_t = cleanlinestransact.map(lambda i: (float(i.split(',')[6]), float(i.split(',')[5])))
date_d = time_t.map(lambda (a,b): (time.strftime("%y.%m", time.gmtime(a)), (b,1)))
t_time = date_d.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda j: (j[0], (j[1][0]/j[1][1]))) #returns average gas price over time

fin = t_time.sortByKey(ascending=True)
fin.saveAsTextFile('AverageGas')


blocks = cleanlinescontract.map(lambda k: (k.split(',')[3], 1)) #block number from contracts


blockdifference = cleanlinesblock.map(lambda b: (b.split(',')[0], (int(b.split(',')[3]), int(b.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(b.split(',')[7])))))) #block number, difficulty, gas used, time from blocks dataset
results = blockdifference.join(blocks).map(lambda (id, ((a, b, c), d)): (c, ((a,b), d)))
final = results.reduceByKey(lambda ((a1,b1), c1) , ((a2, b2), c2): ((a1 + a2, b1 + b2), c1+c2)).map(lambda z: (z[0], (float(z[1][0][0]/z[1][1]), z[1][0][1]/ z[1][1]))).sortByKey(ascending=True) #for all block numbers existing in contracts, returns complexity and gas used
final.saveAsTextFile('Complexity')
