import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep
import csv
import sys,getopt

rawdata=[]
uniqueDates=[]
uniqueSymboles=[]
d_data=[]
trade_matrix=[]
holding_matrix=[]
values_matrix=[]

def test_msg():
    print "Hello"
    df = pd.DataFrame(index=['1','2'], columns=['A', 'B', 'C', 'D'])
    df = df.fillna(0)
    # print df

def read_csv(argv):
    reader = csv.reader(open(argv[1], 'rU'), delimiter=',')
    global rawdata
    for row in reader:
        #print row
        rawdata.append(row) 

def preprocess_raw_data():
    dates = []
    symbols = []
    for row in rawdata:
        dates.append(dt.datetime(int(row[0]), int(row[1]), int(row[2])))
        symbols.append(row[3])

    global uniqueDates
    global uniqueSymbols
    
    uniqueDates = list(set(dates))
    uniqueSymbols = list(set(symbols))

def perform_step2():
    global uniqueDates
    uniqueDates.sort()
    # print uniqueDates

    dt_start = uniqueDates[0]
    dt_end = uniqueDates[-1] + dt.timedelta(days=1)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    dataobj = da.DataAccess('Yahoo')
    ls_symbols = uniqueSymbols
    # ls_symbols = dataobj.get_symbols_from_list('sp5002008')
    # ls_symbols.append('SPX')
    ls_symbols.append('_CASH')

    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)

    global d_data
    d_data = dict(zip(ls_keys, ldf_data))

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)

    d_data['_CASH'] = 1.0
    uniqueDates = [0] + uniqueDates

    #pd.set_printoptions(max_columns=7)
    #print d_data['close'].values
    #d_data.to_csv(sys.stdout)
    
    #print d_data['close']['_CASH']
    #print d_data['close']['AAPL']
    #print d_data['close']['AAPL'][0]
    #print d_data['close']['AAPL'][1]
    #print d_data['close']['AAPL']["2011-01-12 16:00:00"]

def generate_trade_matrix(capital):
    global uniqueDates
    dt_start = uniqueDates[0]
    dt_end = uniqueDates[-1] + dt.timedelta(days=1)

    # We need closing prices so the timestamp should be hours=16.
    dt_timeofday = dt.timedelta(hours=16)

    # Get a list of trading days between the start and the end.
    #ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)

    global trade_matrix
    trade_matrix = pd.DataFrame(index=uniqueDates, columns=uniqueSymbols)
    trade_matrix = trade_matrix.fillna(0)

    trade_matrix.loc[0]["_CASH"] = capital
    previous_date = 0
    for date in uniqueDates:
        #print "-", date
        if(date != 0):
            trade_matrix.loc[date]["_CASH"] = 0
        for row in rawdata:
            if(dt.datetime(int(row[0]),int(row[1]),int(row[2])) == date):
                # print "-", row[3],"-", row[4],"-", row[5]
                # print d_data['close'][row[3]][date+dt.timedelta(hours=16)]
                a = trade_matrix.loc[date][row[3]]
                b = int(row[5]) if row[4] == "Buy" else -1 * int(row[5])

                #if(row[3] == 'GOOG'):
                #    print date
                #    print a
                #    print b
                #    print a + b
                    
                trade_matrix.loc[date][row[3]] = a + b
                
                #if(row[3] == 'GOOG'):
                #    print trade_matrix.loc[date][row[3]]

                trade_matrix.loc[date]["_CASH"] = \
                    trade_matrix.loc[date]["_CASH"] - \
                        b * d_data['close'][row[3]][date+dt.timedelta(hours=16)]
                if(row[3] == 'GOOG'):
                    print "--", date, b, trade_matrix.loc[date]["_CASH"]
                #print trade_matrix.loc[previous_date]["_CASH"]
        previous_date = date    

def generate_holding_matrix():
    global uniqueDates
    uniqueDates = uniqueDates

    global holding_matrix
    holding_matrix = pd.DataFrame(index=uniqueDates, columns=uniqueSymbols)
    holding_matrix = holding_matrix.fillna(0)

    #global trade_matrix
    #print uniqueDates[1]
    #print trade_matrix.loc[uniqueDates[1]].cumsum()
    #df = []
    print "--"
    for symbol in uniqueSymbols:
        holding_matrix[symbol]=trade_matrix[symbol].cumsum()

def calculate_returns():
    global values_matrix
    values_matrix = pd.DataFrame(index=uniqueDates, columns=uniqueSymbols)
    values_matrix = values_matrix.fillna(0)

    for date in uniqueDates:
        c = 0
        if date != 0:
            #print d_data['close'].loc[date+dt.timedelta(hours=16)]
            #print holding_matrix.loc[date]
            a = d_data['close'].loc[date+dt.timedelta(hours=16)]
            b = holding_matrix.loc[date]
            c += a.mul(b)
            values_matrix.loc[date]=c
            #print c
        else:
            values_matrix.loc[date]=holding_matrix.loc[0]
            #print

def write_csv(filename):
    writer = csv.writer(open(filename, 'wb'), delimiter=',')
    for date in uniqueDates:
        if(date!=0):
            val = values_matrix.loc[date].cumsum()['_CASH']
            list_a = [date.year, date.month, date.day, int(val)]
            print list_a
            writer.writerow(list_a)
        
if __name__ == '__main__':
    test_msg()

    read_csv(sys.argv[1:])
    # print rawdata
    preprocess_raw_data()
    # print uniqueDates
    #print ""
    #print uniqueSymbols
    perform_step2()
    # print d_data['close']['AAPL']
    generate_trade_matrix(int(sys.argv[1]))
    print "Trading Matrix"
    print trade_matrix
    generate_holding_matrix()
    print "Holding Matrix"
    print holding_matrix
    calculate_returns()
    print "Value Matrix"
    print values_matrix
    write_csv(sys.argv[3])
