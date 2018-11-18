from socket import *
import subprocess
from string import *
import time 
from threading import *
from struct import *
import sys

storage = {}
Thread_setting = True
tsem = None
serverSocket = None
mutex_lock = None
thread_srv = None
start = None


def netpipe_receiver():
    global Thread_setting
    global serverSocket
    global storage
    buf_counter = -1 #Last packet in FIFO sequence received
    
    doubles = 0 # counter for received packets that have been received before 
    not_fit = 0 #counter for received packets that didn't fit in the storage
    while Thread_setting:
        try:
            #server receives and saves message, (blocks for timeout secs ->tries again)
            message, address = serverSocket.recvfrom(1024)

            #in case server did receive, unpack id for FIFO, filetype for managing the data, and the data
            if(message != ("")):
                (messid,) = unpack('!i',message[0:4])
                (filetype,)=unpack('!c',message[4:5])
                (message,) = unpack('!{}s'.format(len(message[5:])), message[5:])
                messid = int(messid)
                #Check if packet is already in storage or has been read
                if(messid in storage) or (messid < storage["fd"]):
                    #in that case send ACK
                    serverSocket.sendto(str(messid), address)
                    #add one counter for re-received packets
                    if messid!= -1 :
                        doubles += 1
                #Else check if packet fits in storage
                
                elif len(storage) + (messid - buf_counter) <= storage["buffsize"]:
                    #put it in the storage 
                    storage[messid] = []
                    storage[messid].append(filetype)
                    storage[messid].append(message)
                    #send ACK
                    serverSocket.sendto(str(messid), address)
                    
                    #N slots of the storage unit have been locked
                    #when the next packet in FIFO sequences is put in, we unlock one slot
                    #(or multiple slots if we missed it in previous loop)
                    if (messid - buf_counter == 1) and (messid != -1):
                        buf_counter += 1
                        tsem.release()
                        temp = messid
                        mutex_lock.acquire()
                        for i in range(messid,messid + (storage["buffsize"]-1)):
                            if i in storage:
                                if i - temp == 1:
                                	tsem.release()
                                	temp = i
                                	buf_counter += 1
                        mutex_lock.release()
                else:
                    #In case wanted packet arrives but there is no space in storage unit, add 1 to counter
                    not_fit += 1
        except timeout:
        	pass
    
    
#we get the port number after binding the socket and return it to the application
def netpipe_rcv_open(my_address, buffsize):
    global serverSocket, mutex_lock, thread_srv
    global tsem
    global thread_srv
    global start
    global storage
    global Thread_setting

    #Create and bind socket
    try:
        serverSocket = socket(AF_INET, SOCK_DGRAM)
        serverSocket.settimeout(0.5)
        serverSocket.bind((my_address, 0))
    except Exception:
        fd = -1
        print "Exception Occured"
        port = 0
        return fd, port

    port = serverSocket.getsockname()[1]

    #Initialize buffer(storage unit for packets)
    fd = 0
    storage["buffsize"] = buffsize + 2
    storage["fd"] = fd
    mutex_lock = None
    mutex_lock = Lock()
    tsem = None
    tsem = Semaphore()
    tsem.acquire()
    Thread_setting = True
    thread_srv = None
    thread_srv = Thread(target=netpipe_receiver)

    thread_srv.start()
    start = time.time()
    return fd, port


#buf argument is global
def netpipe_read(fd, lenofpack):
    global storage
    
    #A Semaphore is used to know if the packet we want to read is in storage else we block
    tsem.acquire()
    buf = storage[fd]
    
    #In case whole data series fits in server's packet
    if len(buf[1]) <= lenofpack:
        #mutex used for changing storage size in different threads
        mutex_lock.acquire()
        del storage[fd]
        mutex_lock.release()
        
        #move to next packet in FIFO sequence
        fd += 1
        storage["fd"] = fd
            
        return fd, buf
    else:
        #In case server's packet is smaller that data series size 
        #keep len data and "remove"it
        temp = buf[1][0:lenofpack]
        buf[1] = buf[1][lenofpack:] 
        buf_temp=['D',temp]
        #there has been made an unwanted acquire, we didnt fully read a "fd" file
        tsem.release()
        return fd, buf_temp
    
    
def netpipe_rcv_close(fd):
    global Thread_setting
    global thread_srv
    global storage
    
    #Close thread's loop
    Thread_setting=False
    #Bring in initial state
    fd = -1
    storage = {}
    #Close socket
    try:
    	serverSocket.close()
    except Exception:
    	fd = 0
    	print "Exception Occured"
    	return fd
    
    thread_srv.join()
    
    end = time.time()
    total_time = end-start

    return fd
