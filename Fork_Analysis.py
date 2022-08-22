import pyspark
import time

sc = pyspark.SparkContext()


def transactions_clean(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            month = time.strftime("%B",time.gmtime(float(fields[6])))
            year = time.strftime("%Y",time.gmtime(float(fields[6])))
            if month == "October" and year == "2017": #Byzantium fork date
                float(fields[6])
                float(fields[5])
            else:
                return False
            #return True
        return True
    except:
        return False


transactions = sc.textFile("/data/ethereum/transactions")

Tfilter = transactions.filter(transactions_clean)

mapper1 = Tfilter.map(lambda n: (float(n.split(',')[6]), float(n.split(',')[5]))) #Date, gas price
price = mapper1.map(lambda (a,b): (time.strftime("%d.%m.%y", time.gmtime(a)), (b,1)))
effect_on_price = price.reduceByKey(lambda (a1, b1), (a2, b2): (a1+a2, b1+b2)).map(lambda j: (j[0], (j[1][0]/j[1][1]))) #Calculates average gas price for each day in the month of october 2017
effect_on_price.saveAsTextFile('EffectOnPrice')
