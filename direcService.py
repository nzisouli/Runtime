from socket import *
import subprocess
from struct import *
clients = None
s_tcp = None
group = {}
id_counter = 0

def change_udp(id_code, udp_port, client):
	global group

	group[id_code][0][2] = udp_port
	payload = pack('!i', 0)
	client[0].send(payload)

def return_udp(tcp_port, tcp_addr, client):
	global group

	for i in group:
		if group[i][0][0] == tcp_addr and group[i][0][1] == tcp_port:
			payload = pack('!i', 0) + pack('!i', group[i][0][2])
			client[0].send(payload)
			return

def shutdown(id_client, client):
	global group

	group[id_client][2] = False

	for i in group:
		if group[i][2] == True:
			payload = pack('!i', 0) + pack('!i', group[i][0][1]) + pack('!{}s'.format(len(group[i][0][0])), group[i][0][0])
			client[0].send(payload)
			return
	payload = pack('!i', 1)
	client[0].send(payload)


def createTuple(tuplename, id_client, client):
	global group

	group[id_client][1].append(tuplename)
	payload = pack('!i', 0)
	client[0].send(payload)

def findTuple(tuplename,client):
	global group

	peer_info = None
	for i in group:
		if tuplename in group[i][1]:
			peer_info = group[i][0]
			break

	if peer_info != None:
		payload = pack('!i', 0) + pack('!i', peer_info[1]) + pack('!{}s'.format(len(peer_info[0])), peer_info[0])
		client[0].send(payload)
	else:
		payload = pack('!i', 1)
		client[0].send(payload)

def join(tcp_port, udp_port, client_addr, client):
	global group, id_counter

	peer_info = [client_addr, tcp_port, udp_port]
	print "New Member: "+ client_addr + ", tcp port: " + str(tcp_port)
	#Insert new member in group
	group[id_counter] = [peer_info,[], True]
	#Send OK to user
	payload = pack('!i', id_counter)
	id_counter += 1
	client[0].send(payload)

def Main():
	global clients, s_tcp
	clients = [] #storage for clients

	#find my address
	arg='ip route list'
	p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE)
	data = p.communicate()
	sdata = data[0].split()
	my_address = sdata[ sdata.index('src')+1 ]

	#open tcp connection listening 5 clients
	s_tcp = socket()
	s_tcp.bind((my_address, 0))
	port = s_tcp.getsockname()[1]
	s_tcp.settimeout(0.5)
	s_tcp.listen(5)

	#give my info
	print "========================="
	print "Port Number: ", port
	print "IP Address: ", my_address
	print "========================="
	#first listening
	c_tcp=None
	while c_tcp == None:
		try:
			c_tcp, addr = s_tcp.accept() #accept a connection
			c_tcp.settimeout(0.5)
			clients.append([c_tcp, addr])#every new client appends on clients
		except timeout:
			pass

	#Start receiving requests
	while True:
		for client in clients:#for every one that is connected
			try:
				data = client[0].recv(1024)
				if data:
					(code,) = unpack('!i', data[:4])
					#if a join request is received
					if code == 0:
						(tcp_port,) = unpack('!i', data[4:8])
						(udp_port,) = unpack('!i', data[8:12])
						(client_addr,) = unpack('!{}s'.format(len(data[12:])), data[12:])
						join(tcp_port, udp_port, client_addr, client)
					#if a find request is received
					if code == 1:
						(tuplename,) = unpack('{}s'.format(len(data[4:])), data[4:])
						findTuple(tuplename, client)
					if code == 2:
						(id_client,) = unpack('!i', data[4:8])
						(tuplname,) = unpack('{}s'.format(len(data[8:])), data[8:])
						createTuple(tuplename, id_client, client)
					if code == 3:
						(id_code,) = unpack('!i', data[4:8])
						(udp_port,) = unpack('!i', data[8:12])
						change_udp(id_code, udp_port, client)
					if code == 4:
						(tcp_port,) = unpack('!i', data[4:8])
						(tcp_addr,) = unpack('!{}s'.format(len(data[8:])), data[8:])
						return_udp(tcp_port, tcp_addr, client)
					if code == 5:
						(id_client,) = unpack('!i', data[4:8])
						shutdown(id_client, client)
					data = None
			except timeout:
				pass
		#try to connect with a new user
		try:
			c_tcp, addr = s_tcp.accept() #accept a connection
			c_tcp.settimeout(0.5)
			if c_tcp not in clients:
				clients.append([c_tcp,addr])
		except timeout:
			pass

if __name__ == '__main__' :
	Main()
    