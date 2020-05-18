import sys
from pyspark import SparkContext
from collections import Counter
import re

if __name__ == '__main__':
    sc = SparkContext()
    #streets = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=False).cache() #'/data/share/bdm/complaints.csv'
    all_tickets = sc.textFile('/data/share/bdm/nyc_parking_violation/2015.csv', use_unicode=False).cache()
    
    def extractScores(partId, records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        for row in reader:
            if row[4] != '' and row[21] != '' and row[23] != '' and row[24] != '':
                (date, county, house_number, street_name) = (row[4], row[21], row[23], row[24])
                d = int(date.split('/')[2])
                temp = re.findall(r'\d+',house_number)
                res = list(map(int,temp))
                if len(res) > 0:
                    #idd = findid(county, street_name, res)
                    if (d >= 2015 and d <= 2019):
                        yield (d)
    
    ticket = all_tickets.mapPartitionsWithIndex(extractScores)
    ticket.take(10)
    #test = ticket.map(lambda x: ((x[0]), {x[1]: 1} )) \
    #.reduceByKey(lambda x,y: (Counter(x) + Counter(y))) \
    #.mapValues(lambda x: ([i for i in x.values()], len(x.keys()))) \
    #.mapValues(lambda x: (x[0], (x[0][-1]- x[0][0])/ x[1]))
    
    #test.saveAsTextFile('finale')
