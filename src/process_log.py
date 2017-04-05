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
    Function: Calculate the most active addresses and output the file to output_path.
    
    """
    
    # group the data by address and count the appearing time and take the top 10 write to file.
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
    
    """
    Input: file_path
    Output: Void
    Function: Calculate the most popular resources and output the file to output_path.
    
    """
    
    #  group the data by requests and sum all the data rows with the same request type 
    #  then take the top 10 write to the output file 
    
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
    
    """
    Input: file_path
    Output: Void
    Function: Calculate the most popular hours and output the file to output_path.
    
    """
    
    #  group the data by requests and sum all the data rows with the same request type 
    #  then take the top 10 write to the output file 
    
    counter = 0
    counterdict = dict()
    #  convert the timestamp to int to speed up the processing
    time = data['time'].astype('int').tolist() 
    #  convert the time to seconds
    time = [t // 1000000000 for t in time]
    # record the initial time
    initial = data['time'][0]
    #  record the initial and the final time 
    first, last =  time[0], time[-1]
    # make 2 pointers to record the current status 
    # use cnt to record appearing times 
    head, tail, cnt = 0, 0, 0
    seconds = first
    #  first loop through the log time for every second 
    while seconds <= last:
        #  if the tail pointer no in the end of the list and within the interval of 1 hour
        #  move the tail to the next place and increase the cnt 
        while tail < len(time) and time[tail] - seconds <= 3600:
            cnt +=1
            tail += 1
        #  store the cnt to a dictionary 
        counterdict[seconds] = cnt
        #  Check if the head is the same 
        # then update the cnt by -1 and increase the head 
        while head < len(time) and time[head] == seconds:
            cnt -= 1
            head += 1
        seconds += 1
        #  track the process
        counter +=1
        #  show the process evey 10000 times
        if counter % 10000 == 0:
            print counter
        
    # sort the dictionay first by the value then by the key 
    sortedsec = [(k,v) for k, v in sorted(counterdict.iteritems(), key=lambda(k, v): (-v, k))]
    
    
    #  output the result to the file 
    try:
    
        with open(output_path, 'wb') as output:
            for  time_stamp, count in sortedsec[:10]:
                timed = initial + timedelta( seconds = (time_stamp - first))
                output.write('%s -0400,%s\n' % (timed.strftime("%d/%b/%Y:%H:%M:%S"), int(count)))
    except EnvironmentError:
        print 'The output path (hours.txt) may be Wrong! Please Double Check.' 
    
    print 'Feature 3 finished.'

# Feature 4
def blocked_log(output_path,log_data):
    
    """
    Input: file_path
    Output: Void
    Function: Record the logs that should be blocked.
    
    """
    # Created address dictionary to store address as key and time, appeared time and blocked status.
    addressdict = dict()
    # Created blocked list to record the final results.
    blockedlist = []
    count = 0
    # Conver the pandas series to python list to speed up the processing.
    time = log_data['time'].tolist()
    address = log_data['address'].tolist()
    status = log_data['status'].tolist()
    request = log_data['request'].tolist()
    filesize = log_data['filesize'].tolist()
    
    # Loop through the time series

    for i in range(len(time)):
        # show the process every 10000 interations.
        count +=1 
        if count %10000 == 0:
            print count
        host_address = address[i]
        curr_time = time[i]
        #  if the status code is not int then ignore the data.
        try:
            status_code = int(status[i])
        except:
            continue

        # if the address is blocked in currtime add to block list. 
        if host_address in addressdict:
            if addressdict[host_address][3] and (curr_time - addressdict[host_address][0]).total_seconds() <= 300:
                blockedlist.append('%s - - [%s -0400] "%s" %s %s\n' % (host_address, curr_time.strftime("%d/%b/%Y:%H:%M:%S"), request[i], status[i], filesize[i]))
                continue
        # If it is not login fail and never appeared before then continue.
        if status_code != 401 and host_address not in addressdict:
            continue
        
        # If Login success and it is not blocked then clear the blocking status.
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
            #  login fail after 20 seconds.
            if (curr_time - window_start_time).total_seconds() > 20:
                #  Login fail after the second fail after 20 seconds 
                if (curr_time - addressdict[host_address][1]).total_seconds() >20:
                    addressdict[host_address] = (curr_time,curr_time,1,False)
                else:
                    # Start with the second fail in 20 seconds period.
                    addressdict[host_address] = (addressdict[host_address][1],curr_time,2,False)
            #  Next fail happend during the 20 second interval. 
            else:
                if  addressdict[host_address][2] == 1:
                    addressdict[host_address] = (window_start_time,curr_time,2,False)
                # set the blocked status to true.
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

    