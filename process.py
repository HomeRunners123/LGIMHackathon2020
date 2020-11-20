import csv
import json

datafile = "incident_event_log.csv"

fields = {
    "State": {
        "name": "State",
        "column": "incident_state",
        "map": "by_state"
    },
    "Active/Inactive": {
        "name": "Active/Inactive",
        "column": "active",
        "map": "by_active"
    }
}
    
stats = \
{
     "by_state": {},
     "by_active": {}
}
by_state = {}
by_active = {}
by_opened = {}
by_duration = {}

def ProcessRecord(record):
    for field in fields:
        ProcessField(fields[field], record)

def ProcessField(field, record):
    if record[field["column"]] in stats[field["map"]]:
        stats[field["map"]][record[field["column"]]] = \
            stats[field["map"]][record[field["column"]]] + 1
    else:
        stats[field["map"]][record[field["column"]]] = 1

headers = []
with open(datafile, "r") as fh:
    csvr = csv.reader(fh)
    for row in csvr:
        if len(headers) == 0:
            headers = row
        else:
            record = dict(zip(headers, row))

            ProcessRecord (record)
            
            #print (record)

print (json.dumps(stats))