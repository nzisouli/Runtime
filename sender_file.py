from udp_client import *

def sendFile(filename, port, address):
    
    lengthofpack = 512
    buffsize = 10
    
    #Starting this side of app with initialization and first handshake
    fd = netpipe_snd_open(address,port,buffsize)
    if fd == -1:
        print ("Cannot send file")
        return
    
    #send name of file for opening in receiver side
    buf = ['S', filename]
    length = lengthofpack - sys.getsizeof(fd) - sys.getsizeof(buf[0])
    fd = netpipe_snd_write(fd, buf, length)
    
    #open file to start reading data
    f = open(filename,'rb')
    
    #send data 
    buf[0] = 'D'
    while True:
        buf[1] = f.read(length)
        if buf[1] == '':
            break
        fd = netpipe_snd_write(fd, buf,length)
        
    #send "EOF" for closing file in receiver side
    buf[0] = 'S'
    buf[1] = "EOF"
    fd = netpipe_snd_write(fd, buf,length)
    f.close()


    fd = netpipe_snd_close(fd)
    if fd == 0:
        print("Problem closing udp socket")
        return
