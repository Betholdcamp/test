import sys
from pyspark import SparkContext
#from pyspark.sql import SparkSession
from collections import Counter
import re

if __name__ == '__main__':
    sc = SparkContext()
    #spark = SparkSession(sc)
    streets = sc.textFile('hdfs:///data/share/bdm/nyc_cscl.csv')
    all_tickets = sc.textFile('hdfs:///data/share/bdm/nyc_parking_violation/')
   
    def lines(partId, records):
        if partId==0:
            next(records)
            
        import csv
        reader = csv.reader(records)
        next(reader)
        for row in reader:
            if row[0] != '' and row[2] != '' and row[3] != '' and row[4] != '' and row[5] != '':
                (PHYSICALID, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN, ST_LABEL, BOROCODE, FULL_STREE) = (row[0], row[2], row[3], row[4], row[5], row[10], row[13], row[28])
                L_low1 = re.findall(r'\d+',L_LOW_HN)
                L_low = list(map(int, L_low1))
                L_low = tuple(L_low)
                
                L_high1 = re.findall(r'\d+',L_HIGH_HN)
                L_high = list(map(int, L_high1))
                L_high = tuple(L_high)
                
                R_low1 = re.findall(r'\d+',R_LOW_HN)
                R_low = list(map(int, R_low1))
                R_low = tuple(R_low)
                
                R_high1 = re.findall(r'\d+',R_HIGH_HN)
                R_high = list(map(int, R_high1))
                R_high = tuple(R_high)
                
                borocode = ('Unknown', 'NY' , 'BX', 'K', 'Q', 'R')
                yield (int(PHYSICALID), ST_LABEL, , borocode[int(BOROCODE)],  L_low[-1], L_high[-1])
    
    street_line_left = streets.mapPartitionsWithIndex(lines)
    
    def extractScores(partId, records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        next(reader)
        for row in reader:
            if row[4] != '' and row[21] != '' and row[23] != '' and row[24] != '':
                (date, county, house_number, street_name) = (row[4], row[21], row[23], row[24])
                d = int(date.split('/')[-1])
                temp = re.findall(r'\d+',house_number)
                res = list(map(int,temp))
                res tuple(res)
                if len(res) > 0:
                    if (d >= 2015 and d <= 2019):
                        yield (d, street_name, county, res[-1])
    
    ticket = all_tickets.mapPartitionsWithIndex(extractScores)
    
    sqlContext = SQLContext(sc)
    df_violation = sqlContext.createDataFrame(ticket, ('year', 'street', 'borough', 'housenumber'))
    df_street = sqlContext.createDataFrame(street_line_left, ('ID', 'street', 'borough', 'low', 'high'))
    
    match = [df_violation.borough == df_street.borough,
             df_violation.street == df_street.street,
             (df_violation.housenumber >= df_street.low) and (df_violation.housenumber <= df_street.high)]
    
    together = df_street.join(df_violation, match, 'left').groupby([df_street.ID, df_violation.year]).count()
    together.write \
    .format('com.databricks.spark.csv') \
    .save(sys.argv[1])
    
    #.option('header', 'true') \
