
from mrjob.job import MRJob, MRStep
import re
import time
from datetime import datetime

class partD(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducerTT)] #last reducer sorts the volume of possible fraudulent transactions

    def rangeChecker(self, toCheck, checkAgainst):
        return ((toCheck > checkAgainst*0.95) and (toCheck < checkAgainst*1.05)) #if the values sent and received by the same address are +/-5% similar,
                                                                                    #they are assumed to be fraudulent

    def mapper(self,_,line):
        try:
            fields = line.split(",")
            if (len(fields) == 7): #transaction dataset must have 7 fields
                from_addr = fields[1]
                to_addr = fields[2]
                value = float(fields[3])
                if value > 0 and to_addr != 'null':
                    yield(to_addr,('TO', value)) #returns to_address as key and value of transaction with a frefix 'TO' as value
                    yield(from_addr,('FROM', value)) #returns from_address as key and value of transaction with a frefix 'FROM' as value

        except Exception as e:
            pass

    def reducer(self, addr, items):
        TO = False
        TOTOTAL = 0
        FROMTOTAL = 0
        FROM = False

        count = 0

        for itm in items:
            if TO and FROM:
                TO = False
                FROM = False
                count += 1

            if (itm[0] == 'TO'): TO = True; TOTOTAL += itm[1] #Using the prefix, sum of one way transaction is calculated
            if (itm[0] == 'FROM'): FROM = True; FROMTOTAL += itm[1]

        if (count > 10) and self.rangeChecker(TOTOTAL, FROMTOTAL): #checks if from and to values are similar - i.e with +/-5% range
            yield(None,(addr,count)) #returns the address with similar values in to and from transaction along with the number
                                        #of times such transactions were made/received by that address.

    def reducerTT(self, _, vals):
        topTen = sorted(vals, key=lambda x: x[1], reverse=True)[0:10] #sorts in descending order of the count
        r = 1
        for ranking in topTen:
            yield(r,f'{ranking[0]},{ranking[1]}')
            r += 1

if __name__ == '__main__':
    partD.JOBCONF= { 'mapreduce.job.reduces': '5' }
    partD.run()
