from compiler import *
import globalStr
from threading import *
from socket import *
import subprocess
from struct import *
from receiver_file import *
from sender_file import *

thread_no = 0
toOpen = []
tuplesSp = {}
receiverTH = None
c_tcp = None #socket connected to DS
peer_tcp = None #socket connection to peers
send_tcp = None
my_code = None
udp_receiverTH = None
my_port = None


tuplesSpForeign = {} #ex tuplesSpForeign["tuplename"] = (ip,port)

def openT():
	global toOpen,thread_no
	if toOpen != []:
                
		fname = toOpen[0][0]
		args = toOpen[0][1]
		thread_no += 1
		intvals = toOpen[0][2]
		strvals = toOpen[0][3]
		curLine = toOpen[0][4]
		globalStr.threads[thread_no] = [None,fname,"run",False,None, None]
		print "Running program: " + fname + " with id: " + str(thread_no)
		globalStr.threads[thread_no][0] = Thread(target = exec_file, args = [fname,thread_no,args,intvals,strvals,curLine])
		globalStr.threads[thread_no][0].start()
		toOpen.remove(toOpen[0])

def closeT():
	global send_tcp, c_tcp

	for i in globalStr.threads:
		if globalStr.threads[i][2] == "exit":
			if globalStr.threads[i][3] == True:
				filename = globalStr.immigrateData[i][0]
				intvals = globalStr.immigrateData[i][1]
				strvals = globalStr.immigrateData[i][2]
				curLine = globalStr.immigrateData[i][3]
				address = globalStr.threads[i][4]

				payload = pack('!i', 4) + pack('!i', address[1]) + pack('!{}s'.format(len(address[0])), address[0])
				c_tcp.send(payload)
				data = c_tcp.recv(1024) #receive your code from direcotry service
				if data:
					(code,) = unpack('!i', data[:4])
					if code == 0:
						(udp_port,) = unpack('!i', data[4:])

				send_tcp.connect((address))
				payload = pack('!i', 3) + pack('!i', 0) + pack('!{}s'.format(len(filename)), filename)
				send_tcp.send(payload)
				data = send_tcp.recv(1024) #receive data from direcotry service
				if data:
					(code,) = unpack('!i', data)
					if code != 0:
						print "Error in immigration"
						return
				payload = ""
				for j in intvals:
					payload = payload + j + "," + str(intvals[j]) + ","
				payload = payload[:len(payload)-1]
				packet = pack('!i', 3) + pack('!i', 1) + pack('!{}s'.format(len(payload)), payload)
				send_tcp.send(packet)
				data = send_tcp.recv(1024) #receive data from direcotry service
				if data:
					(code,) = unpack('!i', data)
					if code != 0:
						print "Error in immigration"
						return
				payload = ""
				for j in strvals:
					payload = payload + j  + "," + strvals[j] + ","
				payload = payload[:len(payload)-1]
				packet = pack('!i', 3) + pack('!i', 2) + pack('!{}s'.format(len(payload)), payload)
				send_tcp.send(packet)
				data = send_tcp.recv(1024) #receive data from direcotry service
				if data:
					(code,) = unpack('!i', data)
					if code != 0:
						print "Error in immigration"
						return
				payload = pack('!i', 3) + pack('!i', 3) + pack('!i', curLine)
				send_tcp.send(payload)
				data = send_tcp.recv(1024) #receive data from direcotry service
				if data:
					(code,) = unpack('!i', data)
					if code != 0:
						print "Error in immigration"
						return
				send_tcp.close()
				send_tcp = socket()
				sendFile(filename, udp_port, address[0])
				del globalStr.immigrateData[i]
			del globalStr.threads[i]
			break

def putT():
	global tuplesSp,peer_tcp,c_tcp,my_code, send_tcp
        
	if globalStr.putReq != []:
		request = globalStr.putReq[0]
		tuplename = request[0]
		tup = request[1]
		thread_no = request[2]

		#if tuple is in you
		if tuplename in tuplesSp:
			tuplesSp[tuplename].append(tup)
		#if tuple is in specific foreign	 
		elif tuplename in tuplesSpForeign:
			address = tuplesSpForeign[tuplename]
			send_tcp.connect((address))
                        
			payload = tuplename + ":"
			for i in tup:
				payload += str(i)+","
			payload = payload[:len(payload)-1]
			packet = pack('!i', 1) + pack('!{}s'.format(len(payload)), payload)
			send_tcp.send(packet)

			data = send_tcp.recv(1024) #receive data from direcotry service
			if data:
				(code,) = unpack('!i', data)
				if code == 0:
					pass
		#if you don't know where tuple is
		else:
			payload = pack('!i', 1) + pack('!{}s'.format(len(tuplename)), tuplename)
			c_tcp.send(payload)
			data = c_tcp.recv(1024) #receive data from direcotry service
			if data:
				(code,) = unpack('!i', data[:4])
				if code == 0:
					(port,) = unpack('!i', data[4:8])
					(ipaddr,) = unpack('!{}s'.format(len(data[8:])), data[8:])
					tuplesSpForeign[tuplename] = (ipaddr,port)
					send_tcp.connect((ipaddr,port))

					payload = tuplename + ":"
					for i in tup:
						payload += str(i)+","
					payload = payload[:len(payload)-1]
					packet = pack('!i', 1) + pack('!{}s'.format(len(payload)), payload)
					send_tcp.send(packet)

					data = send_tcp.recv(1024) #receive data from direcotry service
					if data:
						(code,) = unpack('!i', data)
						if code == 0:
							pass
				#if noone has the tuple
				elif code == 1:
					#inform dir_service you have this tuple
					payload = pack('!i', 2) + pack('!i', my_code) + pack('!{}s'.format(len(tuplename)), tuplename)
					c_tcp.send(payload)
					data = c_tcp.recv(1024) #receive data from direcotry service
					if data:
						(code,) = unpack('!i', data)
						#ok create tuple
						if code == 0:
							tuplesSp[tuplename] = []
							tuplesSp[tuplename].append(request[1])
		
		globalStr.putReq.remove(request)
		globalStr.putRep.append(thread_no)
		send_tcp.close()
		send_tcp = socket()

def getT():
	global tuplesSp, send_tcp
	i = 0
	while True:
		if globalStr.getReq != [] and i < len(globalStr.getReq):
			request = globalStr.getReq[i]
			tuplename = request[0]
			tup = request[1]
			thread_no = request[2]
			#if tuple in you
			if tuplename in tuplesSp:
				for j in tuplesSp[tuplename]:
					match = True
					newTuple = []
					count_pos = 0
					if len(tup) == len(j):
						for k in tup:
							if k == "?codeASK?":
								newTuple.append(j[count_pos])
							elif k != j[count_pos]:
								match = False
								break
							else:
								newTuple.append(j[count_pos])
								count_pos +=  1
						if match == True :
							globalStr.getRep[thread_no] = newTuple
							globalStr.getReq.remove(request)
							tuplesSp[tuplename].remove(j)
							return
						if thread_no == -1:
							globalStr.getRep[-2] = None
							globalStr.getReq.remove(request)
			#if tuple in foreign
			elif tuplename in tuplesSpForeign:
				address = tuplesSpForeign[tuplename]
				send_tcp.connect((address))
				payload = tuplename + ":"
				for f in tup:
					payload += str(f)+","
				payload = payload[:len(payload)-1]
				packet = pack('!i', 2) + pack('!{}s'.format(len(payload)), payload)
				send_tcp.send(packet)
				data = send_tcp.recv(1024) #receive data from direcotry service
				send_tcp.close()
				send_tcp = socket()
				if data:
					(code,) = unpack('!i', data[:4])
					if code == 3:
						(tup, ) = unpack('!{}s'.format(len(data[4:])), data[4:])
						newTuple = tup.split(",")
						globalStr.getRep[thread_no] = newTuple
						globalStr.getReq.remove(request)
						return
			#find tuple in dir_service
			else:
				payload = pack('!i', 1) + pack('!{}s'.format(len(tuplename)), tuplename)
				c_tcp.send(payload)
				data = c_tcp.recv(1024) #receive data from direcotry service
				if data:
					(code,) = unpack('!i', data[:4])
					if code == 0:
						(port,) = unpack('!i', data[4:8])
						(ipaddr,) = unpack('!{}s'.format(len(data[8:])), data[8:])
						tuplesSpForeign[tuplename] = (ipaddr,port)
						send_tcp.connect((ipaddr,port))
						payload = tuplename + ":"
						for f in tup:
							payload += str(f)+","
						payload = payload[:len(payload)-1]
						packet = pack('!i', 2) + pack('!{}s'.format(len(payload)), payload)
						send_tcp.send(packet)
						data = send_tcp.recv(1024) #receive data from direcotry service
						send_tcp.close()
						send_tcp = socket()
						if data:
							(code,) = unpack('!i', data[:4])
							if code == 3:
								(tup, ) = unpack('!{}s'.format(len(data[4:])), data[4:])
								newTuple = tup.split(",")
								globalStr.getRep[thread_no] = newTuple
								globalStr.getReq.remove(request)
								return
					#inform dir_service you have this tuple
					elif code == 1:
						payload = pack('!i', 2) + pack('!i', my_code) + pack('!{}s'.format(len(tuplename)), tuplename)
						c_tcp.send(payload)
						data = c_tcp.recv(1024) #receive data from direcotry service
						if data:
							(code,) = unpack('!i', data)
							#ok create tuple
							if code == 0:
								tuplesSp[tuplename] = []
			i +=1
		else:
			return

def runtimefun():
	while True:
		openT()
		closeT()
		putT()
		getT()


def receivePeer ():
	global peer_tcp #socket that receives requests from other peers
	global toOpen ,udp_receiverTH, c_tcp

	peers = [] 
	tcp=None
	tcp, addr = peer_tcp.accept() #accept a connection
	tcp.settimeout(0.5)
	peers.append([tcp, addr])#every new client appends on clients
	peer_tcp.settimeout(0.5)
        
	while True: #continuously receiving put  and get requests for his own tuplenames
		for i in peers: #for each one that has connected
			try:
				data= i[0].recv(1024) #receive data from other member
				if data:
					(code,) = unpack('!i', data[:4])
					if code == 1: #if it is a put request
						(data,) = unpack('!{}s'.format(len(data[4:])), data[4:])
						data = data.split(":")
						tuplename = data[0]
						tup = data[1].split(",")
						#i put it as my own request but as foreign thread
						globalStr.putReq.append([tuplename, tup, -1])
						#wait till this request is managed
						while True:
							#send ok to peer
							if -1 in globalStr.putRep :
								globalStr.putRep.remove(-1)
								payload = pack('!i', 0)
								i[0].send(payload)
								break
					elif code == 2:#if its a get request
						(data,) = unpack('!{}s'.format(len(data[4:])), data[4:])
						data = data.split(":")
						tuplename = data[0]
						tup = data[1].split(",")
						#i put it as my own request but as foreign thread
						globalStr.getReq.append([tuplename, tup, -1])
						#wait till this request is managed
						while True:
							if -1 in globalStr.getRep : # if we have  a valid tuple for answer send it back
								payload = ""
								for k in globalStr.getRep[-1]:
									payload += str(k) + ","
									del globalStr.getRep[-1]
									payload = payload[:len(payload)-1]
									packet = pack('!i', 3) + pack('!{}s'.format(len(payload)), payload)
									i[0].send(packet)
									break
							elif -2 in globalStr.getRep: # if we didnt have something for this peer send message so the peer can ignore this request
								del globalStr.getRep[-2]
								payload = pack('!i', 4)
								i[0].send(payload)
								break
					elif code == 3:
						tempOpen = []
						while True:
							(typ,) = unpack('!i', data[4:8])
							if typ == 0:
                                                                
								(filename,) = unpack('!{}s'.format(len(data[8:])), data[8:])
								tempOpen.append(filename)
								tempOpen.append(None)
								payload = pack('!i', 0)
								i[0].send(payload)
							elif typ == 1:
								(vals,) = unpack('!{}s'.format(len(data[8:])), data[8:])
								intvals = {}
								if vals != "":
									vals = vals.split(",")

									l = 0
									while l < len(vals):
										intvals[vals[l]] = int(vals[l+1])
										l += 2
								tempOpen.append(intvals)
								payload = pack('!i', 0)
								i[0].send(payload)
							elif typ == 2:
								(vals,) = unpack('!{}s'.format(len(data[8:])), data[8:])
								vals = vals.split(",")
								strvals = {}
								l = 0
								while l < len(vals):
									strvals[vals[l]] = vals[l+1]
									l += 2
								tempOpen.append(strvals)
								payload = pack('!i', 0)
								i[0].send(payload)
							elif typ == 3:
								(line,) = unpack('!i', data[8:])
								tempOpen.append(line)
								payload = pack('!i', 0)
								i[0].send(payload)
								break
							data = i[0].recv(1024)
							(code,) = unpack('!i', data[:4])
						while globalStr.file_received == False:
							pass
						globalStr.file_received = False
						toOpen.append(tempOpen)
						udp_receiverTH = None
						globalStr.udp_port = None
						udp_receiverTH = Thread(target = receiveFile)
						udp_receiverTH.start()
						while globalStr.udp_port == None:
							pass
						payload = pack('!i', 3) + pack('!i', my_code) + pack('!i', globalStr.udp_port)
						c_tcp.send(payload)
						data = c_tcp.recv(1024) #receive your code from direcotry service
						if data:
							(code,) = unpack('!i', data)
							if code == 0:
								pass
					data = None
			except timeout:
				pass
		try:
			tcp, addr = peer_tcp.accept() #accept a connection
			tcp.settimeout(0.5)
			peers.append([tcp, addr])#every new client appends on clients
		except timeout:
			pass

def initializeTCP():
	global c_tcp, receiverTH, dir_service,peer_tcp, my_code, send_tcp,udp_receiverTH
       
	port = int(raw_input("Give port of directory service: "))
	ipaddr = raw_input("Give IP of directory service: ")
	#Set directory service's address
	dir_service = (ipaddr, port)
	#Create connection with server
	c_tcp = socket() #scoket for connection with direcotry service
	c_tcp.connect((dir_service))

	#find my address
	arg='ip route list'
	p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE)
	data = p.communicate()
	sdata = data[0].split()
	my_address = sdata[ sdata.index('src')+1 ]

	#Create open connection with other users
	peer_tcp = socket()
	peer_tcp.bind((my_address, 0))
	my_port = peer_tcp.getsockname()[1]
	peer_tcp.listen(5)

	udp_receiverTH = Thread(target = receiveFile)
	udp_receiverTH.start()
	while globalStr.udp_port == None:
		pass

        
	receiverTH = Thread(target = receivePeer)
	receiverTH.start()
	
	send_tcp = socket()

	#send own info to directroy esrvice so other peers can get it too
	payload = pack('!i', 0) + pack('!i', my_port) + pack('!i', globalStr.udp_port) + pack('!{}s'.format(len(my_address)), my_address)
	c_tcp.send(payload)

	data = c_tcp.recv(1024) #receive your code from direcotry service
	if data:
		(code,) = unpack('!i', data)
		if code >= 0:
			my_code = code
			print "=================================="
			print "              MENU"
			print "       run <filename> <args>"
			print " migrate <id> <ip_address> <port>"
			print "            shutdown"
			print "=================================="

def Main():
	global toOpen, c_tcp, my_code
	runtime = Thread(target = runtimefun)
	runtime.start()
	initializeTCP()

	while True:
		command = raw_input()
		command = command.split(" ")
		if command[0] == "run":
			toOpen.append([command[1],command[2:],None,None,0]) #fnmae,args,intvlas,strvlas.curLIne
		elif command[0] == "migrate":
			globalStr.threads[int(command[1])][3]=True  #immigrate =true
			globalStr.threads[int(command[1])][4]=(command[2],int(command[3]))
		elif command[0] == "shutdown":
			payload = pack('!i', 5) + pack('!i', my_code)
			c_tcp.send(payload)
			data = c_tcp.recv(1024) #receive your code from direcotry service
			if data:
				(code,) = unpack('!i', data[:4])
				if code == 0:
					(tcp_port,) = unpack('!i', data[4:8])
					(tcp_addrr,) = unpack('!{}s'.format(len(data[8:])), data[8:])
					for i in globalStr.threads:
						globalStr.threads[i][3]=True  #immigrate =true
						globalStr.threads[i][4]=(tcp_addrr,tcp_port)
				elif code == 1:
					print "Cannot shutdown, no other peer active"
			break
	while True:
		pass

if __name__ == '__main__' :
	Main()