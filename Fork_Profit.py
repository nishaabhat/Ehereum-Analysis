import pyspark
import time

sc = pyspark.SparkContext()

def transaction_before_fork(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            day = time.strftime("%D",time.gmtime(float(fields[6])))
            month = time.strftime("%B",time.gmtime(float(fields[6])))
            year = time.strftime("%Y",time.gmtime(float(fields[6])))
            if month == "October" and year == "2017":
                if day in ["14", "15"]: #Two days prior to fork
                    float(fields[1])
                    float(fields[3])
            #return True
            else:
                return False
            #return True
        return True
    except:
        return False

def transaction_after_fork(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            day = time.strftime("%D",time.gmtime(float(fields[6])))
            month = time.strftime("%B",time.gmtime(float(fields[6])))
            year = time.strftime("%Y",time.gmtime(float(fields[6])))
            if month == "October" and year == "2017":
                if day in ["16", "17"]: #Two days after fork
                    float(fields[2])
                    float(fields[3])
            #return True
            else:
                return False
            #return True
        return True
    except:
        return False

transactions = sc.textFile("/data/ethereum/transactions")


Tfilter1 = transactions.filter(transaction_before_fork)
mapper1 = Tfilter1.map(lambda n: (n.split(',')[1], float(n.split(',')[3]))) #considering from_address to identify who sold at higher price
profit1 = mapper1.map(lambda (a,b): (a, (b,1)))
who_profited1 = profit1.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda z: (z[0], z[1][0]/z[1][1]))
top_ten1 = who_profited1.takeOrdered(10,key = lambda a: -a[1]) #Addresses with highest value of transaction
res1 = sc.parallelize(top_ten1).saveAsTextFile('BeforeFork')

Tfilter2 = transactions.filter(transaction_after_fork)
mapper2 = Tfilter2.map(lambda n: (n.split(',')[2], float(n.split(',')[3]))) #considering to_address to identify who bought at lower price
profit2 = mapper2.map(lambda (a,b): (a, (b,1)))
who_profited2 = profit2.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda z: (z[0], z[1][0]/z[1][1]))
top_ten2 = who_profited2.takeOrdered(10,key = lambda a: -a[1]) #Addresses with highest value of transaction
res2 = sc.parallelize(top_ten2).saveAsTextFile('AfterFork')
