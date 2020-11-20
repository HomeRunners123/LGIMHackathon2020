import csv
from datetime import datetime
import json
import sys

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
    },
    "By User": {
        "name": "By User",
        "column": "caller_id",
        "map": "by_user"
    },
    "By User/Date": {
        "name": "By User/Date",
        "column": "caller_id",
        "column2": "opened_at",
        "column2_format": "date"
    },
    "Opened by Date": {
        "name": "Opened by Date",
        "column": "opened_at",
        "column_format": "date"
    },
    "Closed by Date": {
        "name": "Closed by Date",
        "column": "closed_at",
        "column_format": "date",
        "column_match": "active",
        "column_match_value": "false"
    }
}
    
stats = \
{
     "State": {},
     "Active/Inactive": {},
     "By User": {},
     "By User/Date": {},
     "Opened by Date": {},
     "Closed by Date": {}
}
by_state = {}
by_active = {}
by_opened = {}
by_duration = {}

def ProcessRecord(record):
    for field in fields:
        ProcessField(fields[field], record)

def ProcessField(field, record):
    column = record[field["column"]]
        
    if "column_format" in field:
        if field["column_format"] == "date":
            column = datetime.strptime(column, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d")
    
    if "column_match" in field:
        column_match = record[field["column_match"]]
        column_match_value = field["column_match_value"]
        
        if column_match != column_match_value: return

    if "column2" in field:
        column2 = record[field["column2"]]
        
        if "column2_format" in field:
            if field["column2_format"] == "date":
                column2 = datetime.strptime(column2, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d")
        
        if column in stats[field["name"]]:
            if column2 in stats[field["name"]]:
                stats[field["name"]][column][column2] = stats[field["name"]][column][column2] + 1
            else:
                stats[field["name"]][column][column2] = 1
        else:
            stats[field["name"]][column] = {}
            stats[field["name"]][column][column2] = 1
    else:
        if column in stats[field["name"]]:
            stats[field["name"]][column] = \
                stats[field["name"]][column] + 1
        else:
            stats[field["name"]][column] = 1

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