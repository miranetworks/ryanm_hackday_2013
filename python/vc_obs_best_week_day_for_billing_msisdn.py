from disco.core import Job, result_iterator
from datetime import datetime
import time
import csv, sys

def map(line, params):

    AnalyzedLine = line.split(',')

    if AnalyzedLine[0] == 'msisdn':
        TimeStr = AnalyzedLine[1]
        Msisdn  = AnalyzedLine[2]
        Status  = AnalyzedLine[4]
        Count   = AnalyzedLine[6]

        WeekDays = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']


        if Status == 'SUCCESS':
            DateTime = datetime.datetime.fromtimestamp(int(TimeStr))
            # Date = str(DateTime).split()[0]
            DayOfWeek = DateTime.weekday()
            
            for i in range (0, int(Count)):
                yield Msisdn, WeekDays[DayOfWeek]
    
def reduce(iter, params):
    from disco.util import kvgroup
    for key, counts in kvgroup(sorted(iter)):
        Day = ''
        Num = 0
        DayList = list(counts)
        Days = set(DayList)
        for j in Days:
            if DayList.count(j) > Num:
                Num = DayList.count(j)
                Day = j
        
        if Num > 1:
            yield key, Day

if __name__ == '__main__':
    job = Job().run(input=["data:vcobssplit"],
                    map=map,
                    reduce=reduce)
    
    output_filename = "output.csv"

    if len(sys.argv) > 1:
        output_filename = sys.argv[1]

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp)
        for key, date in result_iterator(job.wait(show=True)):
            writer.writerow([key] + [date])

