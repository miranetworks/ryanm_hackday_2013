from disco.core import Job, result_iterator
from datetime import datetime
import time
import csv, sys

def map(line, params):
    # REPORT line looks something like this:
    # 2013-02-15T00:00:39.329+00,REPORT,v5,A7DE11D877021E21A0A8000BCDAB22CE,pi_2006_43475_b,PI 2006 43475,live3,A6BBC59877021E2188CA28CFDAE0DF60,2013022498758757,6077807483,27760901139,499,SUCCESS,,P,smsmt,2013-02-15T00:00:37.583+00,1,65501,S,
    # DateTime,REPORT,Version,GUID1,BulkAccount,ObsSvcId,ObsServer,Guid2,Guid3,Guid4,MSISDN,Cost,Status,Reason,SubsType,Source,StartDateTime,HistNwID,NetworkID,BillType,Token
    # [0],     [1],   [2],    [3],  [4],        [5],     [6],      [7],  [8],  [9],  [10],  [11],[12],  [13],  [14],    [15],  [16],         [17],    [18],     [19],    [20]

    ReportLine = line.split(',')

    if ReportLine[1] == 'REPORT':
        EndDateTime   = ReportLine[0]
        BulkAccount   = ReportLine[4]
        ObsSvcId      = ReportLine[5]
        ObsServer     = ReportLine[6]
        Msisdn        = ReportLine[10]
        Cost          = ReportLine[11]
        Status        = ReportLine[12]
        Reason        = ReportLine[13]
        SubsType      = ReportLine[14]
        Source        = ReportLine[15]
        StartDateTime = ReportLine[16]
        NetworkID     = ReportLine[18]
        BillType      = ReportLine[19]

        DateAndHour = StartDateTime.split(':')[0]
        ParsedDateTime = datetime.datetime.strptime(DateAndHour,"%Y-%m-%dT%H")
        Time = time.mktime(ParsedDateTime.timetuple())
        TimeString = str(round(Time)).rstrip('0').rstrip('.')

        yield ('obs',TimeString,NetworkID,'report'), 1
        yield ('billtype_bulkaccount',TimeString,BulkAccount,BillType), 1
        yield ('obs_svc_id',TimeString,ObsSvcId), 1
        yield ('obs_svc_id_status',TimeString,ObsSvcId,Cost,Status), 1
        yield ('msisdn',TimeString,Msisdn,Cost,Status,Reason), 1
        if Status != 'SUCCESS':
            yield ('fail_reason',TimeString,Reason), 1
    
def reduce(iter, params):
    from disco.util import kvgroup
    for key, counts in kvgroup(sorted(iter)):
        yield key, sum(counts)

if __name__ == '__main__':
    job = Job().run(input=["data:vcobs"],
                    map=map,
                    reduce=reduce)
    
    output_filename = "output.csv"

    if len(sys.argv) > 1:
        output_filename = sys.argv[1]

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp)
        for key, count in result_iterator(job.wait(show=True)):
            writer.writerow(list(key) + [str(count)])

