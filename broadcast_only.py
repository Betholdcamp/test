import sys
from pyspark import SparkContext
from collections import Counter
import re

if __name__ == '__main__':
    sc = SparkContext()
    streets = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=False).cache() #'/data/share/bdm/complaints.csv'
    #all_tickets = sc.textFile('/data/share/bdm/nyc_parking_violation/', use_unicode=False).cache()
    
    def lines(partId, records):
        if partId==0:
            next(records) 
        
        import csv
        reader = csv.reader(records)
        for row in reader:
            if row[0] != '' and row[2] != '' and row[3] != '' and row[4] != '' and row[5] != '':
                (PHYSICALID, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN, ST_LABEL, BOROCODE, FULL_STREE) = (row[0], row[2], row[3], row[4], row[5], row[10], row[13], row[28])
                L_low1 = re.findall(r'\d+',L_LOW_HN)
                L_low = list(map(int, L_low1))
                
                L_high1 = re.findall(r'\d+',L_HIGH_HN)
                L_high = list(map(int, L_high1))
                
                R_low1 = re.findall(r'\d+',R_LOW_HN)
                R_low = list(map(int, R_low1))
                
                R_high1 = re.findall(r'\d+',R_HIGH_HN)
                R_high = list(map(int, R_high1))
                
                borocode = ('Unknown', 'NY' , 'BX', 'K', 'Q', 'R')
                yield (int(PHYSICALID), (L_low, L_high), (R_low, R_high), ST_LABEL, borocode[int(BOROCODE)], FULL_STREE)
    
    street_line = streets.mapPartitionsWithIndex(lines)
    street_list = sc.broadcast(street_line)
