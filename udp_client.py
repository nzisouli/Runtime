import time
from socket import *
import sys
from threading import *
from struct import *

storage = {}
Thread_setting = True
tsem = None
flush_lock = None
mutex_lock = None
clientSocket = None
flow_stop = None #counter for packets that are blocked and wait for free space in storage unit
thread_cl = None
start = None


def netpipe_sender(server_addr):
    global Thread_setting
    global clientSocket
    global flow_stop

    resent_counter = 0 #counter for re-sent packets 
    packet_timeout = 2 #static time unit for checking packets that need to be resent
    next_packet = 0 #next packet in FIFO sequence to be sent 
    
    while Thread_setting:
        #In case next packet in FIFO sequence is in storage, send it
        if(next_packet in storage):
            message = storage[next_packet][0] 
            
            clientSocket.sendto(message, server_addr)
            storage[next_packet][1] = time.time()   
            next_packet += 1

        #Every three packets we have sent or if there is no next packet in FIFO sequence
        #send the packets whose timeouts have passed and their id is smaller than the one that we need to send next
        if (next_packet%3 == 0) or (next_packet not in storage):
            mutex_lock.acquire() #mutex used to keep the size of the storage unit intact
            
            for i in storage:
                if i != "buffsize":
                    now = time.time()
                    start_time = storage[i][1]
                    
                    if(start_time+packet_timeout <= now) and (i < next_packet):
                        message = storage[i][0]
                        clientSocket.sendto(message, server_addr)
                        storage[i][1] = time.time()
                        resent_counter += 1
            mutex_lock.release()
            
        #try to receive ack for 0.5 seconds
        try:
            data, server = clientSocket.recvfrom(1024)
            #received ACK and remove packet from buffer
            if data != "":
                
                ack_no= int(data)
                if ack_no in storage:
                    del storage[ack_no]
                    tsem.release()
                    
                    #flush_lock used for netpipe_flush()
                    if (len(storage) == 1) and (flush_lock.locked()):
                        flush_lock.release()
        except timeout:
            pass

    

def netpipe_snd_open(ip_address, port, buffsize):
    global storage,Thread_setting,tsem,flush_lock,mutex_lock,clientSocket,flow_stop,thread_cl,start,flag
    
    storage = {}
    Thread_setting = True
    flush_lock = None
    flush_lock = Lock()
    mutex_lock = None
    mutex_lock = Lock()
    clientSocket = None
    flow_stop = 0 #counter for packets that are blocked and wait for free space in storage unit
    
    flag =0
    
    start = time.time() 
    
    #Create socket
    try:
    	clientSocket = socket(AF_INET, SOCK_DGRAM)
    	clientSocket.settimeout(0.5)
    except Exception:
    	fd = -1
    	print "Exception Occured"
    	return fd
    server_addr = (ip_address, port)

    #testing connection with sending 4 packets with id = -1
    for i in range (4):
        payload = pack('!i',-1) + pack('!c','S')+ pack('!{}s'.format(len("testing")),"testing")
        clientSocket.sendto(payload, server_addr)
        try:
            data, server = clientSocket.recvfrom(1024)
            #received ACK
            if data != "":
                ack_no= data
                if (int(ack_no)== -1 ):
                    flag=1
                    break                
        except timeout:
            pass
        
    # In case of an established connection
    if(flag == 1):        
        #Initialize buffer (storage unit)
        fd = 0
        storage["buffsize"] = buffsize + 1
        tsem = None
        tsem = Semaphore()
        for i in range(buffsize-1):
            tsem.release()
        flush_lock.acquire()
        
        thread_cl = Thread(target=netpipe_sender, args=(server_addr,))
        thread_cl.start()
    else: 
        fd = -1
        print ("Udp Connection Failed")

    return fd
        

def netpipe_snd_write(fd, buf, lenofpack):
    global storage
    global flow_stop

    type_of_packet = buf[0]
    data = buf[1]
    
    if len(storage) == storage["buffsize"]: #In case our storage unit is full, add 1 to flowstop counter
        flow_stop += 1
        
    tsem.acquire()
    mutex_lock.acquire()
    
    payload = pack('!i',fd) + pack('!c',buf[0])+ buf[1] #packing the data with its id for FIFO sequence and datatype
    storage[fd] = [payload, -1]       #second element of list will be used for time
    fd += 1
    
    mutex_lock.release()
    return fd


def netpipe_flush(fd):
    global storage
    
    #In case we call flush and there are still packets in storage, block until storage is empty
    while len(storage) > 1:
    	flush_lock.acquire()

    return fd


def netpipe_snd_close(fd):
    global Thread_setting
    global clientSocket
    global thread_cl

    #flush before closing, always emptying the buffer and sending data across
    fd = netpipe_flush(fd)
    
    #Closing other thread's loop
    Thread_setting=False
    #Bring sotrage unit in initial state
    storage = {}
    fd = -1
    #Close socket
    try:
    	clientSocket.close()
    except Exception:
    	fd = 0
    	print "Exception Occured"
    	return fd
    
    thread_cl.join()
    end = time.time()
    total_time = end-start

    return fd
        
