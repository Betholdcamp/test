import sys
from pyspark import SparkContext
from collections import Counter
import re

if __name__ == '__main__':
    sc = SparkContext()
    streets = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=False).cache() #'/data/share/bdm/complaints.csv'
    all_tickets = sc.textFile('/data/share/bdm/nyc_parking_violation/', use_unicode=False).cache()
    
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
    
    def findid(borough, street, h_num):
        dd = None
        for i in street_list.value:
            if (i[3] == street or i[5] == street) and i[4]==borough:
                if (len(h_num) == 1) and (h_num[0] % 2 == 0) and (h_num[0] >= i[2][0][0] and h_num[0] <= i[2][1][0]):
                    dd = i[0]
                    break
                elif (len(h_num) == 1) and (h_num[0] % 2 != 0) and (h_num[0] >= i[1][0][0] and h_num[0] <= i[1][1][0]):
                    dd = i[0]
                    break
                elif (len(h_num) == 2) and (h_num[1] % 2 == 0) and (len(i[2][0])==2) and (len(i[2][1])==2) and (h_num[1] >= i[2][0][1] and h_num[1] <= i[2][1][1]):
                    dd = i[0]
                    break  
                elif (len(h_num) == 2) and (h_num[1] % 2 != 0) and (len(i[1][0])==2) and (len(i[1][1])==2) and (h_num[1] >= i[1][0][1] and h_num[1] <= i[1][1][1]):
                    dd = i[0]
                    break 
            else:
                dd = None
                break
        return dd
    
    def extractScores(partId, records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        for row in reader:
            if row[4] != '' and row[21] != '' and row[23] != '' and row[24] != '':
                (date, county, house_number, street_name, ) = (row[4], row[21], row[23], row[24])
                d = int(date.split('/')[2])
                temp = re.findall(r'\d+',house_number)
                res = list(map(int,temp))
                if len(res) > 0:
                    idd = findid(county, street_name, res)
                    if idd != None and (d >= 2015 and d <= 2019):
                        yield (idd, d)
    
    ticket = all_tickets.mapPartitionsWithIndex(extractScores)
