from mrjob.job import MRJob
import re
import time
import math

class partA(MRJob):
    def mapper(self, _, line):
         fields = line.split(",")
         try:
             if (len(fields) == 7): #retrive value only when the thransaction dataset has 7 fields
                 time_epoch = int(fields[6])
                 month = time.strftime("%B",time.gmtime(time_epoch)) # %B gets month name
                 year = time.strftime("%Y",time.gmtime(time_epoch)) #gets year
                 yield((month,year),1) #returns the count of transaction per month
         except:
             pass #if the dataset doesn't have 7 fields, ignore it

    def combiner(self, word, values):
        total = sum(values) #sums the total count per key
        yield(word, total)

    def reducer(self, word, values):
        total = sum(values)
        yield(word, total)


if __name__ == '__main__':
    partA.run()
