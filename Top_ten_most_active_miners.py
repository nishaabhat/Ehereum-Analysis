import pyspark

sc = pyspark.SparkContext()

def blocks_clean(line): #fetches only valid data from blocks dataset by checking the number of fields
    try:
        fields = line.split(',')
        if len(fields) == 9:
            str(fields[2])
            if int(fields[4]) == 0: #removes data if size of the block is 0
                return False
        else:
            return False
        return True
    except:
        return False

blocks = sc.textFile('/data/ethereum/blocks').filter(blocks_clean) #fetches only relevant data
bmapp = blocks.map(lambda n: (n.split(',')[2], int(n.split(',')[4]))) #maps to miner and size of the block
b_aggregated = bmapp.reduceByKey(lambda a,b: (a+b)) #aggregates on size by miner field
top10 = b_aggregated.takeOrdered(10, key=lambda x: -x[1]) #descending order

for record in top10:
    print('{0},{1}'.format(record[0],record[1])) #prints on console
