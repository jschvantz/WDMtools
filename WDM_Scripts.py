'''
Created on Feb 13, 2013

@author: justint
'''
import baker
import pandas as pd
import sys
import numpy as np
import csv
import time
import wdmutil
wdm = wdmutil.WDM()



##### Justins additions
@baker.command
def min15tocsv(wdmpath, input, output, *kwds): #start_date=None, end_date=None):
    '''Output monthly aggregated timeseries to monthly aggregated timeseries
    in the following format:
    Metzone,Date,Parm 1,Parm 2,...,Parm n
    Met 1,2000-01-01,.034,.035,...,.031
    Met 1,2000-02-01,.561,.046,...,.041
    ...  ,  ...  ,  ...  ,  ...  ,  ...
    Met n,2009-12-01,.045,.376,...,.184
    :param wdmpath:     Path of name of WDMfile, i.e. "example.wdm"
    :param input:       Input.txt file with dsns listed in a matrix
                            with metzones as the header and dsns for a
                            particular metzone listed below, comma
                            delimitted.
    :param output:      Output.txt file for time series to be exported,
                            exported file is comma delimited, parm as header,
                            metzone as first column, and data for a particular
                            dsn as a matrix.
    :Example String
    '''
    start = time.clock()    #Start record of time process takes
    #Initialize variables
    f = 0
    g = 1
    h = 0
    i = 1
    l = 0
    nParm = 1
    #Read input file of DSNs
    dsns = np.genfromtxt(input, 'str', delimiter=',')
    nMet = len(dsns[0])+1
    start_date = None
    end_date = None
    Remain = len(dsns[0])+1
    print '{0} metzones to be processed.'.format(Remain-1)
    #Create array
    ToWrite = np.zeros((120*len(dsns[0])+1,len(dsns)+1))
    ToWrite = np.asarray(ToWrite, '|S10')
    ToWrite[0,0] = 'MetZone'
    ToWrite[0,1] = 'Dates'
    Dates = pd.date_range('1/1/2000', periods=120, freq='MS')
        
    while f+1 < nMet:
        print 'Metzone {0} is being worked...'.format(dsns[0,f])
        k = 1
        g = 1
        h = 120*f
        if f == 0:
            while nParm < len(dsns):
                dsn = dsns[nParm,0]
                if len(dsn) == 3:
                    ToWrite[0,nParm+1] = '0' + dsn[2] 
                else:
                    ToWrite[0,nParm+1] = dsn[0] + dsn[3]
                nParm = nParm + 1
        while k < len(Dates)+1:
            ToWrite[k+h,0] = dsns[0,f]
            ToWrite[k+h,1] = str(Dates[k-1])
            k = k + 1
        i = 1
        while i < len(dsns):
            print len(dsns)
            dsn = dsns[i,f]
            print dsn
            nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
#            print 'nts = {0}'.format(len(nts))
            if len(nts) > 150000:
                mdates = pd.date_range('1/1/2000',periods=len(nts), freq= '15min')   #Model time step
                data = pd.TimeSeries( nts, index = mdates) #Model time series
                by = lambda x: lambda y: getattr(y,x)
                data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
            elif len(nts) < 150000 and len(nts) >10000:
                mdates = pd.date_range('1/1/2000',periods=len(nts), freq= 'H')   #Model time step
                data = pd.TimeSeries( nts, index = mdates) #Model time series
                by = lambda x: lambda y: getattr(y,x)
                data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
            elif len(nts) < 10000:
                mdates = pd.date_range('1/1/2000',periods=len(nts), freq= 'D')   #Model time step
                data = pd.TimeSeries( nts, index = mdates) #Model time series
                by = lambda x: lambda y: getattr(y,x)
                data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
            print data
            l = 120*f 
            g = 1
            print 'dates+1 = {0}'.format(len(Dates)+1)
            while g < (len(Dates)+1):     #Loop to print monthly results
                ToWrite[g+l,i+1] = str(data[g-1])
                g = g + 1
            print ToWrite
            i = i + 1
        f = f + 1
        Remain = Remain - 1
        if Remain == int(len(dsns[0])*5/6):
            print '17% complete'
        elif Remain == int(len(dsns[0])*4/6):
            print '35% complete'
        elif Remain == int(len(dsns[0])*3/6):
            print '50% complete'
        elif Remain == int(len(dsns[0])*2/6):
                print '67% complete' 
        elif Remain == int(len(dsns[0])*1/6):
            print '81% complete'    
    ForOutput = csv.writer(open(output , 'wb'))
    for row in range(len(ToWrite)):
        ForOutput.writerow(ToWrite[row])

    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes                                   


@baker.command
def dsnsumtometcsv(wdmpath, input, output, *kwds): #start_date=None, end_date=None):
    '''Output monthly aggregated timeseries to monthly aggregated timeseries
    in the following format:
    Metzone,Date,Parm 1,Parm 2,...,Parm n
    Met 1,2000-01-01,.034,.035,...,.031
    Met 1,2000-02-01,.561,.046,...,.041
    ...  ,  ...  ,  ...  ,  ...  ,  ...
    Met n,2009-12-01,.045,.376,...,.184
    :param wdmpath:     Path of name of WDMfile, i.e. "example.wdm"
    :param input:       Input.txt file with dsns listed in a matrix
                            with metzones as the header and dsns for a
                            particular metzone listed below, comma
                            delimitted.
    :param output:      Output.txt file for time series to be exported,
                            exported file is comma delimited, parm as header,
                            metzone as first column, and data for a particular
                            dsn as a matrix.
    :Example String
    '''
    start = time.clock()    #Start record of time process takes
    #Initialize variables
    f = 0
    g = 1
    h = 0
    i = 1
    l = 0
    nParm = 1
    #Read input file of DSNs
    dsns = np.genfromtxt(input, 'str', delimiter=',')
    nMet = len(dsns[0])+1
    start_date = None
    end_date = None
    Remain = len(dsns[0])+1
    print '{0} metzones to be processed.'.format(Remain-1)
    #Create array
    ToWrite = np.zeros((120*len(dsns[0])+1,len(dsns)+1))
    ToWrite = np.asarray(ToWrite, '|S10')
    ToWrite[0,0] = 'MetZone'
    ToWrite[0,1] = 'Dates'
    Dates = pd.date_range('1/1/2000', periods=120, freq='MS')
        
    while f+1 < nMet:
        print 'Metzone {0} is being worked...'.format(dsns[0,f])
        k = 1
        g = 1
        h = 120*f
        if f == 0:
            while nParm < len(dsns):
                dsn = dsns[nParm,0]
                if len(dsn) == 3:
                    ToWrite[0,nParm+1] = '0' + dsn[2] 
                else:
                    ToWrite[0,nParm+1] = dsn[0] + dsn[3]
                nParm = nParm + 1
        while k < len(Dates)+1:
            ToWrite[k+h,0] = dsns[0,f]
            ToWrite[k+h,1] = str(Dates[k-1])
            k = k + 1
        i = 1
        while i < len(dsns):
            dsn = dsns[i,f]
            nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
            mdates = pd.date_range('1/1/2000',periods=len(nts))   #Model time step
            data = pd.TimeSeries( nts, index = mdates)  #Model time series
            by = lambda x: lambda y: getattr(y,x)
            data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
            l = 120*f
            g = 1
            while g < len(Dates)+1:     #Loop to print monthly results
                ToWrite[g+l,i+1] = str(data[g-1])
                g = g + 1
            i = i + 1
        f = f + 1
        Remain = Remain - 1
        if Remain == int(len(dsns[0])*5/6):
            print '17% complete'
        elif Remain == int(len(dsns[0])*4/6):
            print '35% complete'
        elif Remain == int(len(dsns[0])*3/6):
            print '50% complete'
        elif Remain == int(len(dsns[0])*2/6):
                print '67% complete' 
        elif Remain == int(len(dsns[0])*1/6):
            print '81% complete'    
    ForOutput = csv.writer(open(output , 'wb'))
    for row in range(len(ToWrite)):
        ForOutput.writerow(ToWrite[row])

    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes                                   

@baker.command    
def dsnmeantometcsv(wdmpath, input, output, *kwds): #start_date=None, end_date=None):
    '''Output monthly aggregated timeseries to monthly aggregated timeseries
    in the following format:
    Metzone,Date,Parm 1,Parm 2,...,Parm n
    Met 1,2000-01-01,.034,.035,...,.031
    Met 1,2000-02-01,.561,.046,...,.041
    ...  ,  ...  ,  ...  ,  ...  ,  ...
    Met n,2009-12-01,.045,.376,...,.184
    :param wdmpath:     Path of name of WDMfile, i.e. "example.wdm"
    :param input:       Input.txt file with dsns listed in a matrix
                            with metzones as the header and dsns for a
                            particular metzone listed below, comma
                            delimitted.
    :param output:      Output.txt file for time series to be exported,
                            exported file is comma delimited, parm as header,
                            metzone as first column, and data for a particular
                            dsn as a matrix.
    :Example String
    '''
    start = time.clock()    #Start record of time process takes
    #Initialize variables
    f = 0
    g = 1
    h = 0
    i = 1
    l = 0
    nParm = 1
    
    #Read input file of DSNs
    dsns = np.genfromtxt(input, 'str', delimiter=',')
    nMet = len(dsns[0])+1
    start_date = None
    end_date = None
#    start_date = kwds.setdefault('start_date', None)
#    end_date = kwds.setdefault('end_date', None)
    Remain = len(dsns[0])+1
    print '{0} metzones to be processed.'.format(Remain-1)
    
    #Create array
    ToWrite = np.zeros((120*len(dsns[0])+1,len(dsns)+1))
    ToWrite = np.asarray(ToWrite, '|S10')
    ToWrite[0,0] = 'MetZone'
    ToWrite[0,1] = 'Dates'
    Dates = pd.date_range('1/1/2000', periods=120, freq='MS')
        
    while f+1 < nMet:
        print 'Metzone {0} is being worked...'.format(dsns[0,f])
        k = 1
        g = 1
        h = 120*f
        if f == 0:
            while nParm < len(dsns):
                dsn = dsns[nParm,0]
                if len(dsn) == 3:
                    ToWrite[0,nParm+1] = '0' + dsn[2] 
                else:
                    ToWrite[0,nParm+1] = dsn[0] + dsn[3]
                nParm = nParm + 1
        while k < len(Dates)+1:
            ToWrite[k+h,0] = dsns[0,f]
            ToWrite[k+h,1] = str(Dates[k-1])
            k = k + 1
        i = 1
        while i < len(dsns):
            dsn = dsns[i,f]
            nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
            mdates = pd.date_range('1/1/2000',periods=len(nts))   #Model time step
            data = pd.TimeSeries( nts, index = mdates)  #Model time series
            by = lambda x: lambda y: getattr(y,x)

            data = data.groupby([by('year'), by('month')]).apply(lambda x: np.mean(x))   #Original time series aggregated monthly
            l = 120*f
            g = 1
            while g < len(Dates)+1:     #Loop to print monthly results
                ToWrite[g+l,i+1] = str(data[g-1])
                g = g + 1
            i = i + 1
        f = f + 1
        Remain = Remain - 1
        if Remain == int(len(dsns[0])*5/6):
            print '17% complete'
        elif Remain == int(len(dsns[0])*4/6):
            print '35% complete'
        elif Remain == int(len(dsns[0])*3/6):
            print '50% complete'
        elif Remain == int(len(dsns[0])*2/6):
                print '67% complete' 
        elif Remain == int(len(dsns[0])*1/6):
            print '81% complete'    

    ForOutput = csv.writer(open(output , 'wb'))
    
    for row in range(len(ToWrite)):
        ForOutput.writerow(ToWrite[row])

    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes                                   
    
@baker.command
def dsnstocols(wdmpath, input, output, *oper,**kwds): #start_date=None, end_date=None):
    '''Output monhtly aggregated timeseries for specified DSNs into a three col array.
    :param wdmpath:     Path or name of WDM file, i.e. "example.wdm" .
    :param input:       Input.txt file with dsns listed in a single column.
    :param output:      Output.txt file for time sereies to be exported to,
                            note that this function produces a DSN,Data,Time 
                            column output with DSNs following each other.
    
    :Example String:        wdmtoolbox.py dsnstocols example.wdm inputfile.txt outputfile.txt    
    by: justint
    ''' 
    start = time.clock()    #Start record of time process takes
    
    dsns = open(input,'r').readlines()  #Open input file and store in list
    oper = str(oper)
    start_date = kwds.setdefault('start_date', None)
    end_date = kwds.setdefault('end_date', None)
    
    ForAccess = csv.writer(open(output , 'wb'))
    ForAccess.writerow(['DSN','Date','Value'])   #Write headers to CSV file
    Remain = len(dsns)
    print '{0} DSNs to be processed.'.format(Remain)
    
    
    
    for dsn in dsns:   #loop for DSNs listed in the input file
#        header = _describedsn(wdmpath, dsn)
        
        nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
        mdates = pd.date_range('1/1/2000',periods=len(nts))   #Model time step
        data = pd.TimeSeries( nts, index = mdates)  #Model time series
        by = lambda x: lambda y: getattr(y,x)
        data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
        mdates = pd.date_range('1/1/2000',periods=len(data),freq='MS')    #New monthly time steps
        for i in range(len(data)):     #Loop to print monthly results
            ForAccess.writerow([int(dsn), mdates[i], data[i]])

        Remain = Remain - 1
        if Remain == int(len(dsns)*5/6):
            print '17% complete'
        elif Remain == int(len(dsns)*4/6):
            print '35% complete'
        elif Remain == int(len(dsns)*3/6):
            print '50% complete'
        elif Remain == int(len(dsns)*2/6):
            print '67% complete' 
        elif Remain == int(len(dsns)*1/6):
            print '81% complete'    
    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes

@baker.command
def dsnstocsv(wdmpath,*dsns,**kwds): #start_date=None, end_date=None):
    '''Print DSN(s) to screen with YYYY-MM-DD date formats and values.
    :param wdmpath: Path or name of WDM file, i.e. "example.wdm" .
    :param dsns:    The DSNs that you wish to save to CSV seperated
                        by a space, i.e. "2101 2102 6101" .
    
    :Example String:    wdmtoolbox.py dsnstocsv example.wdm 2101 2102 6101    
    '''    

    start_date = kwds.setdefault('start_date', None)
    end_date = kwds.setdefault('end_date', None)
    for dsn in dsns:
        nts = wdm.read_dsn(wdmpath, int(dsn), start_date=start_date, end_date=end_date)
        for i in range(len(nts)):
            odates = pd.date_range('1/1/2000',periods=len(nts))
            print '{0},{1},{2}'.format(dsn, odates[i] ,nts[i])

@baker.command
def dailytocols(wdmpath, input, output, *kwds): #start_date=None, end_date=None):
    '''Daily timeseries exported to csv
    
    Date,DSN 1,DSN 2,...,DSN n
    2000-01-01,.034,.035,...,.031
    2000-02-01,.561,.046,...,.041
    ...  ,  ...  ,  ...  ,  ...  ,  ...
    2009-12-01,.045,.376,...,.184
    :param wdmpath:     Path of name of WDMfile, i.e. "example.wdm"
    :param input:       Input.txt file with dsns listed in a matrix
                            with metzones as the header and dsns for a
                            particular metzone listed below, comma
                            delimitted.
    :param output:      Output.txt file for time series to be exported,
                            exported file is comma delimited, parm as header,
                            metzone as first column, and data for a particular
                            dsn as a matrix.
    :Example String
    '''
    start = time.clock()    #Start record of time process takes
    #Initialize variables
    col = 0
    count = 0
    #Read input file of DSNs
    dsns = np.genfromtxt(input, 'str', delimiter=',')
    start_date = None
    end_date = None
    Remain = len(dsns)+1
    print '{0} DSNs to be processed.'.format(Remain-1)
    #Create array
    ToWrite = np.zeros((3654,len(dsns)+1))
    ToWrite = np.asarray(ToWrite, '|S10')
    ToWrite[0,0] = 'Dates'
    for dsn in dsns:
        row = 0
        nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
        period = pd.date_range('1/1/2000',periods=len(nts))   #Model time step
#         for line in period:
#             print line
        data = pd.TimeSeries( nts, index=period)  #Model time series
        #by = lambda x: lambda y: getattr(y,x)
        #data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
        ToWrite[0,col+1] = str(dsn)
        while row < len(nts):     #Loop to print monthly results
            if col == 0:
                ToWrite[row+1,0] = str(period[row])
                count = count + 1
            ToWrite[row+1,col+1] = str(data[row])
            row = row + 1
        col = col + 1
        Remain = Remain - 1
        if Remain == int(len(dsns)*5/6):
            print '17% complete'
        elif Remain == int(len(dsns)*4/6):
            print '35% complete'
        elif Remain == int(len(dsns)*3/6):
            print '50% complete'
        elif Remain == int(len(dsns)*2/6):
                print '67% complete'
        elif Remain == int(len(dsns)*1/6):
            print '81% complete'
    
    ForOutput = csv.writer(open(output , 'wb'))
    for line in range(len(ToWrite)):
        ForOutput.writerow(ToWrite[line])

    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes    
@baker.command
def monthlytocols(wdmpath, input, output, *kwds): #start_date=None, end_date=None):
    '''Daily timeseries exported to csv
    
    Date,DSN 1,DSN 2,...,DSN n
    2000-01-01,.034,.035,...,.031
    2000-02-01,.561,.046,...,.041
    ...  ,  ...  ,  ...  ,  ...  ,  ...
    2009-12-01,.045,.376,...,.184
    :param wdmpath:     Path of name of WDMfile, i.e. "example.wdm"
    :param input:       Input.txt file with dsns listed in a matrix
                            with metzones as the header and dsns for a
                            particular metzone listed below, comma
                            delimitted.
    :param output:      Output.txt file for time series to be exported,
                            exported file is comma delimited, parm as header,
                            metzone as first column, and data for a particular
                            dsn as a matrix.
    :Example String
    '''
    start = time.clock()    #Start record of time process takes
    #Initialize variables
    col = 0
    count = 0
    #Read input file of DSNs
    dsns = np.genfromtxt(input, 'str', delimiter=',')
    start_date = None
    end_date = None
    Remain = len(dsns)+1
    print '{0} DSNs to be processed.'.format(Remain-1)
    #Create array
    ToWrite = np.zeros((121,len(dsns)+1))
    ToWrite = np.asarray(ToWrite, '|S10')
    ToWrite[0,0] = 'Dates'
    for dsn in dsns:
        row = 0
        nts = wdm.read_dsn(wdmpath, int(int(dsn)), start_date=start_date, end_date=end_date)    #calls values for a particular DSN
        print len(nts)
        period = pd.date_range('1/1/2000',periods=120, freq='MS')   #Model time step
        print len(period)
#         for line in period:
#             print line
        data = pd.TimeSeries( nts, index=period)  #Model time series
        print data
        #by = lambda x: lambda y: getattr(y,x)
        #data = data.groupby([by('year'), by('month')]).apply(lambda x: np.sum(x))   #Original time series aggregated monthly
        ToWrite[0,col+1] = str(dsn)
        while row < len(nts):     #Loop to print monthly results
            if col == 0:
                ToWrite[row+1,0] = str(period[row])
                count = count + 1
            ToWrite[row+1,col+1] = str(data[row])
            row = row + 1
        col = col + 1
        Remain = Remain - 1
        if Remain == int(len(dsns)*5/6):
            print '17% complete'
        elif Remain == int(len(dsns)*4/6):
            print '35% complete'
        elif Remain == int(len(dsns)*3/6):
            print '50% complete'
        elif Remain == int(len(dsns)*2/6):
                print '67% complete'
        elif Remain == int(len(dsns)*1/6):
            print '81% complete'
    
    ForOutput = csv.writer(open(output , 'wb'))
    for line in range(len(ToWrite)):
        ForOutput.writerow(ToWrite[line])

    print '100% complete'
    print 'Completion time was {0} mins.'.format((time.clock()-start)/60)    #End record of time process takes   

@baker.command
def stdtowdm(wdmpath, dsn, infile='-'):
    ''' Writes data from a CSV file to a DSN.
    :param wdmpath: Path and WDM filename (<64 characters).
    :param dsn: The Data Set Number in the WDM file.
    :param infile: Input filename, defaults to standard input.
    '''
    tsd = tsutils.read_iso_ts(baker.openinput(infile))
    __writetodsn(wdmpath, dsn, tsd.dates, tsd)

@baker.command
def inputdsns(wdmpath, input):
    ''' Writes data from a CSV file to a DSN.\n
    File can have comma separated\n
    'year', 'month', 'day', 'hour', 'minute', 'second', 'value'\n
    OR\n
    'date/time string', 'value'
    :param wdmpath: Path and WDM filename (<64 characters).
    :param dsn: The Data Set Number in the WDM file.
    :param input: Input filename, defaults to standard input.
    '''
    dsns = open(input,'r').readlines()  #Open input file and store in list
    Remain = len(dsns)
    
    for dsn in dsns:
        dates = np.array([],'str')
        data = np.array([],'str')
        read = np.genfromtxt(str(int(dsn))+'.csv', 'str', delimiter=',')
        for line in read:
            dates = np.append(dates, str(line[0]))
            data = np.append(data, str(line[1]))
        print dates
        print data
        
        __writetodsn(wdmpath, dsn, dates, data)
           
def __writetodsn(wdmpath, dsn, dates, data):
    # Convert string to int
    dsn = int(dsn)
    # Find ALL unique intervals in the data set and convert to seconds
    import numpy as np
    import numpy.ma as ma
   
    if not isinstance(dates[0], datetime.datetime):
        dates = np.array(dates.tolist())
    interval = np.unique(dates[1:] - dates[:-1])
    interval = [i.days*86400 + i.seconds for i in interval]
   
    # If there are more than one interval lets see if the are caused by
    # missing values.  Say there is at least one 2 hour interval and at
    # least one 1 hour interval, this should correctly say the interval
    # is one hour.
    ninterval = {}
    if len(interval) > 1:
        for i, aval in enumerate(interval):
            for j, bval in enumerate(interval[i+1:]):
                ninterval[__find_gcf(aval, bval)] = 1
        ninterval = ninterval.keys()
        ninterval.sort()
        interval = ninterval
   
    # If the number of intervals is STILL greater than 1
    if len(interval) > 1:
        raise MissingValuesInInputError
    interval = interval[0]
   
    # Have the interval in seconds, need to convert to
    # scikits.timeseries interval identifier and calculate time step.
    freq = 'YEAR'
    tstep = 1
    if interval < 3600 and interval % 60 == 0:
        freq = 'MINUTE'
        tstep = interval/60
    elif interval < 86400 and interval % 3600 == 0:
        freq = 'HOUR'
        tstep = interval/3600
    # The DAY and MONTH tests are not the best, could be fooled by day interval
    # with monthly/yearly time steps.
    elif interval not in 86400*np.array([28, 29, 30, 31, 365, 366]) and interval % 86400 == 0:
        freq = 'DAY'
        tstep = interval/86400
    elif interval in 86400*np.array([28, 29, 30, 31]):
        freq = 'MONTH'
        tstep = int(round(interval/29.5))
   
    # Make sure that input data interval match target DSN
    desc_dsn = __describedsn(wdmpath, dsn)
    dsntcode = desc_dsn['tcode']
    if wdmutil.MAPFREQ[freq] != dsntcode:
        raise FrequencyDoesNotMatchError
   
    # Write the data...
    data = ts.time_series(data, dates, freq=freq)
    wdm.write_dsn(wdmpath, dsn, data, dates[0])


#         dates = np.zeros((len(read[0]),1))
#         data = np.zeros((len(read),1))
#         for line in read:
            
#             data[line,0] = float(read[line,1])
#             print dates
#         print data
#   
#           
#           
#     if isinstance(input, basestring):
#         input = open(input, 'r')
#     dates = np.array([])
#     data = np.array([])
#     for line in input:
#         if '#' == line[0]:
#             continue
#         words = line.split(',')
#         if len(words) == 2:
#             if words[0]:
#                 dates = np.append(dates, dateparser(words[0], default=datetime.datetime(2000,1,1)))
#                 data = np.append(data, float(words[1]))
#         elif len(words) == 7:
#             for i in range(6):
#                 try:
#                     words[i] = int(words[i])
#                 except ValueError:
#                     words[i] = 0
#             data = np.append(data, float(words[6]))
#             dates = np.append(dates, datetime.datetime(*words[:6]))
#   
#     sorted_indices = np.argsort(dates)
#     dates = dates[sorted_indices]
#     data = data[sorted_indices]
#     __writetodsn(wdmpath, dsn, dates, data)
#   
# def __writetodsn(wdmpath, dsn, dates, data):
#     # Convert string to int
#     dsn = int(dsn)
#     # Find ALL unique intervals in the data set and convert to seconds
#     import numpy as np
#     import numpy.ma as ma
#   
#     if not isinstance(dates[0], datetime.datetime):
#         dates = np.array(dates.tolist())
#     interval = np.unique(dates[1:] - dates[:-1])
#     interval = [i.days*86400 + i.seconds for i in interval]
#   
#     # If there are more than one interval lets see if the are caused by
#     # missing values.  Say there is at least one 2 hour interval and at
#     # least one 1 hour interval, this should correctly say the interval
#     # is one hour.
#     ninterval = {}
#     if len(interval) > 1:
#         for i, aval in enumerate(interval):
#             for j, bval in enumerate(interval[i+1:]):
#                 ninterval[__find_gcf(aval, bval)] = 1
#         ninterval = ninterval.keys()
#         ninterval.sort()
#         interval = ninterval
#   
#     # If the number of intervals is STILL greater than 1
#     if len(interval) > 1:
#         raise MissingValuesInInputError
#     interval = interval[0]
#   
#     # Have the interval in seconds, need to convert to
#     # scikits.timeseries interval identifier and calculate time step.
#     freq = 'YEAR'
#     tstep = 1
#     if interval < 3600 and interval % 60 == 0:
#         freq = 'MINUTE'
#         tstep = interval/60
#     elif interval < 86400 and interval % 3600 == 0:
#         freq = 'HOUR'
#         tstep = interval/3600
#     # The DAY and MONTH tests are not the best, could be fooled by day interval
#     # with monthly/yearly time steps.
#     elif interval not in 86400*np.array([28, 29, 30, 31, 365, 366]) and interval % 86400 == 0:
#         freq = 'DAY'
#         tstep = interval/86400
#     elif interval in 86400*np.array([28, 29, 30, 31]):
#         freq = 'MONTH'
#         tstep = int(round(interval/29.5))
#   
#     # Make sure that input data interval match target DSN
#     desc_dsn = __describedsn(wdmpath, dsn)
#     dsntcode = desc_dsn['tcode']
#     if wdmutil.MAPFREQ[freq] != dsntcode:
#         raise FrequencyDoesNotMatchError
#   
#     # Write the data...
#     data = ts.time_series(data, dates, freq=freq)
#     wdm.write_dsn(wdmpath, dsn, data, dates[0])

###################################################### END JUSTIN'S ADDITIONS ####################################################