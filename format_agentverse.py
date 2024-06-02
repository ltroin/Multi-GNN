import numpy as np
import datatable as dt
from datetime import datetime
from datatable import f,join,sort
import sys
import os

n = len(sys.argv)

if n == 1:
    print("No input path")
    sys.exit()

inPath = sys.argv[1]
outPath = os.path.dirname(inPath) + "/formatted_transactions_agentverse_moreatt.csv"

raw = dt.fread(inPath, columns = dt.str32)

currency = dict()
paymentFormat = dict()
bankAcc = dict()
account = dict()

######## below is our new attributes ########
TransactionAuthorizationMethod= dict()
TransactionFees= dict()
DestinationBankID= dict()
DestinationCustomerName= dict()
TransactionOrigin= dict()
TransactionStatus= dict()

def get_dict_val(name, collection):
    if name in collection:
        val = collection[name]
    else:
        val = len(collection)
        collection[name] = val
    return val


# header = "EdgeID,from_id,to_id,Timestamp,\
# Amount Received,Received Currency,\
# Payment Format,Is Laundering\n"

######## below is our new attributes ########
header = "EdgeID,from_id,to_id,Timestamp,\
Amount Received,Received Currency,\
Payment Format,Is Laundering,\
Transaction Authorization Method,\
Transaction Fees,\
Destination Bank ID,\
Transaction Origin,\
Transaction Status\n"

firstTs = -1

with open(outPath, 'w') as writer:
    writer.write(header)
    for i in range(raw.nrows):
        datetime_object = datetime.strptime(raw[i,"Timestamp"], '%Y-%m-%d %H:%M:%S')
        ts = datetime_object.timestamp()
        day = datetime_object.day
        month = datetime_object.month
        year = datetime_object.year
        hour = datetime_object.hour
        minute = datetime_object.minute

        if firstTs == -1:
            startTime = datetime(year, month, day)
            firstTs = startTime.timestamp() - 10

        ts = ts - firstTs

        cur1 = get_dict_val(raw[i,"Currency"], currency)

        fmt = get_dict_val(raw[i,"TransactionType"], paymentFormat)

        # fromAccIdStr = raw[i,"From Bank"] + raw[i,2]
        fromId = get_dict_val(raw[i,"AccountNumber"], account)

        # toAccIdStr = raw[i,"To Bank"] + raw[i,4]
        toId = get_dict_val(raw[i,"DestinationAccountID"], account)

        amountReceivedOrig = float(raw[i,"Amount"])

        isl = int(raw[i,"money_laundering"])


        ######## below is our new attributes ########
        TransactionAuthorizationMethod_temp = get_dict_val(raw[i,"TransactionAuthorizationMethod"], TransactionAuthorizationMethod)
        TransactionFees_temp = get_dict_val(raw[i,"TransactionFees"], TransactionFees)
        DestinationBankID_temp = get_dict_val(raw[i,"DestinationBankID"], DestinationBankID)
        DestinationCustomerName_temp = get_dict_val(raw[i,"DestinationCustomerName"], DestinationCustomerName)
        TransactionOrigin_temp = get_dict_val(raw[i,"TransactionOrigin"], TransactionOrigin)
        TransactionStatus_temp = get_dict_val(raw[i,"TransactionStatus"], TransactionStatus)

        # line = '%d,%d,%d,%d,%f,%d,%d,%d\n' % \
        #             (i,fromId,toId,ts, amountReceivedOrig,cur1,fmt,isl)
        line = '%d,%d,%d,%d,%f,%d,%d,%d,%d,%f,%d,%d,%d\n' % \
                    (i,fromId,toId,ts, amountReceivedOrig,cur1,fmt,isl,TransactionAuthorizationMethod_temp,TransactionFees_temp,DestinationBankID_temp,TransactionOrigin_temp,TransactionStatus_temp)

        writer.write(line)

formatted = dt.fread(outPath)
formatted = formatted[:,:,sort(3)]

formatted.to_csv(outPath)

