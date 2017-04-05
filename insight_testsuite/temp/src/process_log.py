from __future__ import with_statement
import re
import pandas as pd
import sys
import heapq
from datetime import timedelta 
import collections
import operator
import time

def data_preprocessing(input_path):
    
    """
    Input: file_path
    Output: pandas dataframe
    
    """
    
    #  use pandas to read in file and do the data preprocessing
    data = pd.read_csv(input_path,sep = " ", names=["address", "useless", "useless", "time", "useless", "request", "status", "filesize"], index_col=False)
    #  drop the useless colums 
    data = data.drop([column for column in data.columns if column.startswith('useless')], axis=1)
    #  conver string of time to datetime 
    data.time = data.time.str[1:]
    data.time = pd.to_datetime(data.time, format='%d/%b/%Y:%H:%M:%S')
    # check the filesize and drop the useless row
    data.filesize = data.filesize.astype(str)
    data = data.drop(data[data.filesize.apply(lambda e: False if re.match(r'\d+', e) or e == '-' else True)].index)
    data.ix[data.filesize=='-', 'filesize'] = 0
    data.filesize = data.filesize.astype(int)
    print 'Data preprocessing finished.'
    return data


# Feature 1

def active_address(output_path,data):
    """
    Input: file_path
    Output: Void
    Function: Calculate the most active addresses and output the file to 
    
    """
    
    top10active_address = data.groupby('address')['time'].count().sort_values(ascending=False).head(10)
    
    try:
        with open(output_path, 'wb') as output:
            for host, count in top10active_address.iteritems():
                output.write('%s,%s\n' % (host, count))    
    except EnvironmentError:
        print 'The output path (host.txt) may be Wrong! Please Double Check.'
    
    print 'Feature 1 Finished.'


# Feature 2

def active_resource(output_path,data):
    
    top10active_resource = data.groupby('request')['filesize'].sum().sort_values(ascending=False).head(10)
    
    
    try:
        with open(output_path, 'wb') as output:
            for request, _ in top10active_resource.iteritems():
                url = request.split()[1]
                output.write('%s\n' % url)
                
    except EnvironmentError:
        print 'The output path (hours.txt) may be Wrong! Please Double Check.'
    
    print 'Feature 2 Finished.'
    



#  Feature 3 
#  Record the most busiesthour 

def busiesthour(output_path,data):
    counter = 0
    counterdict = dict()
    #  convert the time to 
    time = data['time'].astype('int').tolist() 
    time = [t // 1000000000 for t in time]
    print time[0], time[-1]
    initial = data['time'][0]
    first, last =  time[0], time[-1]
    head, tail, cnt = 0, 0, 0
    seconds = first
    while seconds <= last:
        while tail < len(time) and time[tail] - seconds <= 3600:
            cnt +=1
            tail += 1
        counterdict[seconds] = cnt
        while head < len(time) and time[head] == seconds:
            cnt -= 1
            head += 1
        seconds += 1
        counter +=1
        if counter % 10000 == 0:
            print counter
        
    sortedsec = [(k,v) for k, v in sorted(counterdict.iteritems(), key=lambda(k, v): (-v, k))]
    print 'Feature 3 finished.'
    
    try:
    
        with open(output_path, 'wb') as output:
            for  time_stamp, count in sortedsec[:10]:
                timed = initial + timedelta( seconds = (time_stamp - first))
                output.write('%s -0400,%s\n' % (timed.strftime("%d/%b/%Y:%H:%M:%S"), int(count)))
    except EnvironmentError:
        print 'The output path (hours.txt) may be Wrong! Please Double Check.' 
        		
# Feature 4
def blocked_log(output_path,log_data):
    addressdict = dict()
    blockedlist = []
    count = 0
    time = log_data['time'].tolist()
    address = log_data['address'].tolist()
    status = log_data['status'].tolist()
    request = log_data['request'].tolist()
    filesize = log_data['filesize'].tolist()

    for i in range(len(time)):
        count +=1 
        if count %10000 == 0:
            print count
        host_address = address[i]
        curr_time = time[i]
        try:
            status_code = int(status[i])
        except:
            continue

        # if the address is blocked in currtime add to block list 
        if host_address in addressdict:
            if addressdict[host_address][3] and (curr_time - addressdict[host_address][0]).total_seconds() <= 300:
                blockedlist.append('%s - - [%s -0400] "%s" %s %s\n' % (host_address, curr_time.strftime("%d/%b/%Y:%H:%M:%S"), request[i], status[i], filesize[i]))
                continue
        
        if status_code != 401 and host_address not in addressdict:
            continue
        
            
        if status_code == 200 and host_address in addressdict:
            addressdict.pop(host_address,None)
            continue
        
            
        #  first time come to the error status code 401
        if status_code == 401 and host_address not in addressdict:
            # put curr_time to the tuple in dict as the start time to start the 20s window
            addressdict[host_address] = (curr_time,curr_time,1,False)
            continue
        # with error code and address already in the address dict
        elif status_code == 401:
            window_start_time  = addressdict[host_address][0]
            if (curr_time - window_start_time).total_seconds() > 20:
                if (curr_time - addressdict[host_address][1]).total_seconds() >20:
                    addressdict[host_address] = (curr_time,curr_time,1,False)
                else:
                    addressdict[host_address] = (addressdict[host_address][1],curr_time,2,False)
            else:
                if  addressdict[host_address][2] == 1:
                    addressdict[host_address] = (window_start_time,curr_time,2,False)
                elif addressdict[host_address][2] == 2:
                     addressdict[host_address] = (curr_time,curr_time,0,True)
                else:
                    continue
        else:
            continue
    try:
        
        with open(output_path,'wb') as output:
            output.writelines(blockedlist)
            
    except EnvironmentError:
        print 'The output path (blocked.txt) may be Wrong! Please Double Check.'   
    
    print 'Feature 4 finished.'


def main():
    
    # Get input and output file path
	input_path = sys.argv[1]
	
	hosts_path = sys.argv[2]
	
	resources_path = sys.argv[3]
	
	hours_path = sys.argv[4]
	
	blocked_path = sys.argv[5] 
	
	# Readin log.txt file
	data = data_preprocessing(input_path) 
	
	# Output host.txt
	active_address(hosts_path,data)

    # Output resources.txt
	active_resource(resources_path,data)

    # Output hours.txt
	busiesthour(hours_path,data)
	
    # Output blocked.txt
	blocked_log(blocked_path,data)



if __name__ == "__main__":
    main()

    