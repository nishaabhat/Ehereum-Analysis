import pyspark

def transactions_clean(line): #fetches only valid data from transactions dataset by checking the number of fields
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False


def contracts_clean(line): #fetches only valid data from contracts dataset by checking the number of fields
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
contracts = sc.textFile("/data/ethereum/contracts")

transaction_filter = transactions.filter(transactions_clean)
transaction_mapper = transaction_filter.map(lambda n: (n.split(',')[2], float(n.split(',')[3]))) #creates an RDD in the form of to_address, value of transaction

contract_filter  = contracts.filter(contracts_clean)
contract_mapper = contract_filter.map(lambda a: (a.split(',')[0],1)) #fetches first field from contracts. i.e. address

transaction_reducer = transaction_mapper.reduceByKey(lambda a,b: (a + b)) #aggregates on to_address

final = transaction_reducer.join(contract_mapper).map(lambda a: (a[0], a[1][0])) #joins on to_address from transactions and address from contracts

top_ten = final.takeOrdered(10,key = lambda a: -a[1]) #sorts in decending order and fetches top 10 values

for top in top_ten:
    print("{}: {}".format(top[0], top[1])) #prints address and total ether on console
