from mrjob.job import MRJob
import re
import time
import math

class partA_avg(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
             if (len(fields) == 7): #retrive value only when the thransaction dataset has 7 fields
                 time_epoch = int(fields[6])
                 transaction_value = int(fields[3])
                 month = time.strftime("%B",time.gmtime(time_epoch))
                 year = time.strftime("%Y",time.gmtime(time_epoch))
                 yield((month,year),transaction_value) #returns transaction values per month
         except:
             pass #if the dataset doesn't have 7 fields, ignore it


    def combiner(self, word, values):
        count = 0
        total = 0
        for value in values:
            count += 1 #increments count of transaction
            total += value #sums up the value of transaction
        yield (word, (total, count))

    def reducer(self, word, values):
        count = 0
        total = 0
        for item in values:
            count = count + item[1]
            total = total + item[0]
        average = total/count #calculates average transaction value per month
        yield(word, average)


if __name__ == '__main__':
    partA_avg.run()
