from time import *
import globalStr

intvals = {} #structure for numeric variables for all threads
labels = {} #structrure matching line with label for all threads
strvals = {}#structure for string variables for all threads


def setvar(command,thread_no):
	global intvals,strvals
	check_eos = False #flag to see is valid
	if command[1][0] == "$":	#if var
		#find varvalI1
		if command[2][0] == "$": #search it in intvals and strvals
			if command[2] in intvals[thread_no]:
				val = intvals[thread_no][command[2]]
				intvals[thread_no][command[1]] = val
			elif command[2] in strvals[thread_no]:
				val = strvals[thread_no][command[2]]
				strvals[thread_no][command[1]] = val
			else:
				print "Error: Var Not Found!"
				return False
		else: #else see if it is valid value
			try:
				val = int(command[2])
				intvals[thread_no][command[1]] = val
			except:

				if command[2][0] =="\"" and check_eos == False:
					check_eos = True
					variable = command[1]
					command.remove(command[0])
					command.remove(command[0])
					val = ""
					val = command[0][1:]
					if command[0][len(command[0])-1] == "\"" and len(command[0])!=1:
						val = val[:len(val) - 1]
						strvals[thread_no][variable] = val
						check_eos = False
					command.remove(command[0])
					counter_pos = 0
					for i in command:
						counter_pos += 1
						val =val + " " + i 
						if val[len(val)-1]=="\"" and  counter_pos == len(command):
							val = val[:len(val)-1]
							strvals[thread_no][variable] = val
							check_eos = False
					if check_eos != False:
						print "Error: not correct val"
						return False
	else:
		print "Error: not a variable"
		return False
	return True

def arithmetic(command, thread_no): #
	global intvals,strvals
	if command[1][0] == "$":	#if var
		#find varvalI1
		if command[2][0] == "$": #if it is in intvals
			if command[2] in intvals[thread_no]:
				val1 = intvals[thread_no][command[2]]
			else:
				print "Error: Var Not Found!"
				return False
		else: #else if its valid
			try:
				val1 = int(command[2])
			except:
				print "Error: Var Not Int!"
				return False
		#find varvalI2
		if command[3][0] == "$":
			if command[3] in intvals[thread_no]:
				val2 = intvals[thread_no][command[3]]
			else:
				print "Error: Var Not Found!"
				return False
		else:
			try:
				val2 = int(command[3])
			except:
				print "Error: Var Not Int!"
				return False
		#execute command
		if command[0] == "ADD":
			intvals[thread_no][command[1]] = val1 + val2
		elif command[0] == "SUB":
			intvals[thread_no][command[1]] = val1 - val2
		elif command[0] == "MUL":
			intvals[thread_no][command[1]] = val1 * val2
		elif command[0] == "DIV":
			intvals[thread_no][command[1]] = val1 / val2
		elif command[0] == "MOD":
			intvals[thread_no][command[1]] = val1 % val2
		return True
	else:
		print "Error: Not A Var Name"
		return False

def branch(command,thread_no):
	global intvals,strvals,labels
	#find varvalI1
	if command[1][0] == "$":
		if command[1] in intvals[thread_no]:
			val1 = intvals[thread_no][command[1]]
		else:
			print "Error: Var Not Found!"
			return False
	else:
		try:
			val1 = int(command[1])
		except:
			print "Error: Var Not Int!"
			return False

	#find varvalI2
	if command[2][0] == "$":
		if command[2] in intvals[thread_no]:
			val2 = intvals[thread_no][command[2]]
		else:
			print "Error: Var Not Found!"
			return False
	else:
		try:
			val2 = int(command[2])
		except:
			print "Error: Var Not Int!"
			return False

	#find label
	#labels are saved when a program is opened
	if command[3][0] == "#" and command[3] in labels[thread_no]:
		label = labels[thread_no][command[3]]
	else:
		print "Error: Label Not Found!"
		return False

	#execute command
	if command[0] == "BGT":
		if val1 > val2:
			return label
		else:
			return -1
	elif command[0] == "BGE":
		if val1 >= val2:
			return label
		else:
			return -1
	elif command[0] == "BLT":
		if val1 < val2:
			return label
		else:
			return -1
	elif command[0] == "BLE":
		if val1 <= val2:
			return label
		else:
			return -1
	elif command[0] == "BEQ":
		if val1 == val2:
			return label
		else:
			return -1
	
def bra(command,thread_no):
	global labels
	#find label
	if command[1][0] == "#" and command[1] in labels[thread_no]:
		label = labels[thread_no][command[1]]
	else:
		print "Error: Label Not Found!"
		return False
	return label

def put(command,thread_no):
	global intvals,strvals
	check_eos = False #flag for checking valid strings
	
	command.remove(command[0])
	#save tuple name
	if command[0][0] == "$": #search value if it is a variable
		if command[0] in strvals[thread_no]:
			name = strvals[thread_no][command[0]]
		else:
			print "Error: Var Not Found!"
			return False
	else:
		if command[0][0]=="\"" and command[0][len(command[0])-1]=="\"":
			name = command[0][1:len(command[0])-1]
		else:
			print "Error: not correct val"
			return False
            
	command.remove(command[0])
	tupleOb = []
	#put the rest on a tuple
	for i in command: #for each element,find its value
		if i[0] == "$":
			if i in intvals[thread_no]:
				val = intvals[thread_no][i]
			elif i in strvals[thread_no]:
				val = strvals[thread_no][i]
			else:
				print "Error: Var Not Found!"
				return False
		else:
			try:
				if check_eos == False :
					val = int(i)
				else:
					if  i[len(i)-1]=="\"" and check_eos == True:
						val = val + " " + i[:len(i)-1]
						check_eos = False
					else:
						val = val + " " + i 
			except:
				if i[0]=="\"" and check_eos == False:
					val = i[1:]
					check_eos = True
					if i[len(i)-1]=="\"" and len(i)!=1:
						val = val[:len(val) - 1]
						check_eos = False
				elif  i[len(i)-1]=="\"" and check_eos == True:
					val = val + " " + i[:len(i)-1]
					check_eos = False
				elif check_eos == True:
					val = val +" " +i
				else:
					print "Error: not correct val"
					return False
		#if we find the value ,either because the string is complete either because it's an int we put it in tuple
		if check_eos == False :
			try:
				#in case its an int we put it with the code "!i" in front of it
				val = int(val)
				val = "!i" + str(val)
			except:
				pass
			tupleOb.append(val)
			val = ""
	#put in putREQUEST status so the controller can act on it
	globalStr.putReq.append([name,tupleOb,thread_no])	
	
	#wait until controller has manage your request
	while True:
		if thread_no in globalStr.putRep:
			globalStr.putRep.remove(thread_no)
			return True
	
def get(command,thread_no):
	global intvals,strvals
	inputElements = {}
	countEl = 0
	check_eos = False
	command.remove(command[0])

	#Name of tuple space
	if command[0][0] == "$":
		if command[0] in strvals[thread_no]:
			name = strvals[thread_no][command[0]]
		else:
			print "Error: Var Not Found!"
			return False
	else:
		if command[0][0]=="\"" and command[0][len(command[0])-1]=="\"":
			name = command[0][1:len(command[0])-1]
		else:
			print "Error: not correct val"
			return False

	command.remove(command[0])
	tupleOb = []

	#for every tuple element
	for i in command:
		if i[0] == "$": #if it is a given variable give it as an input or as an ouput
			if i in intvals[thread_no]:
				val = intvals[thread_no][i]
			elif i in strvals[thread_no]:
				val = strvals[thread_no][i]
			else: #if we need an output for this variable we put it in the tuple with this specific code
				inputElements[i] = countEl#and we keep the specific position of the tuple for the reurned value
				val = "?codeASK?"
		else:
			try:
				if check_eos == False :
					val = int(i)
				else:
					if  i[len(i)-1]=="\"" and check_eos == True:
						val = val + " " + i[:len(i)-1]
						check_eos = False
					else:
						val = val + " " + i 
			except:
				if i[0]=="\"" and check_eos == False:
					val = i[1:]
					check_eos = True
					if i[len(i)-1]=="\"" and len(i)!=1:
						val = val[:len(val) - 1]
						check_eos = False
				elif i[len(i)-1]=="\"" and check_eos == True:
					val = val + " " + i[:len(i)-1]
					check_eos = False
				elif check_eos == True:
					val = val +" " +i
				else:
					print "Error: not correct val"
					return False
		#when the tuple object ready insert it in tuple
		if check_eos == False :
			try:
				val = int(val)
				val = "!i" + str(val) #code for int values
			except:
				pass
			tupleOb.append(val)
			val = ""

		countEl += 1

	#put in getREQUEST status so the controller can act on it
	globalStr.getReq.append([name,tupleOb,thread_no])	
	
	#wait until controller has manage your request
	while True:
		if thread_no in globalStr.getRep:
			takTuple = globalStr.getRep[thread_no]
			del globalStr.getRep[thread_no]
			for i in inputElements:
				if takTuple[inputElements[i]][:2] == "!i":
					value = takTuple[inputElements[i]][2:]
					value = int(value)
					intvals[thread_no][i] = value
				else:
					strvals[thread_no][i] = takTuple[inputElements[i]]

			return True
    
def slp(command,thread_no):
	global intvals
	if command[1][0] == "$":
		if command[1] in intvals[thread_no]:
			val = intvals[thread_no][command[1]]
		else:
			print "Error: Var Not Found!"
			return False
	else:
		try:
			val = int(command[1])
		except:
			print "Error: Var Not Int!"
			return False
	sleep(val)
	return True

def prn(command,thread_no):
	global intvals,strvals
	check_eos = False
	command.remove(command[0])
	print "From program " + str(thread_no) + ": ",
	for i in command:
		if i[0] == "$":
			if i in intvals[thread_no]:
				val = intvals[thread_no][i]
			elif i in strvals[thread_no]:
				val = strvals[thread_no][i]
			else:
				print "Error: Var Not Found!"
				return False
		else:
			try:
				val = int(i)
			except:
				if i[0]=="\"" and check_eos == False:
					
					val = i[1:]
					check_eos = True
					if i[len(i)-1]=="\"" and len(i)!=1:
						val = val[:len(val) - 1]
						check_eos = False
				elif  i[len(i)-1]=="\"" and check_eos == True:
					val = i[:len(i)-1]
					check_eos = False
				elif check_eos == True:
					val = i
				else:
					print "Error: not correct val"
					return False
		
		print val,

	print ""
	return True

def delete(command,thread_no):
	global intvals,strvals

	command.remove(command[0])

	if command[0][0] == "$" :
		if command[0] in intvals[thread_no]:
			del intvals[thread_no][command[0]]
			return True
		elif command[0] in strvals[thread_no]:
			del strvals[thread_no][command[0]]
			return True
		else:
			print "Error: var not found"
			return False
	else:
		print "Error : not var"
		return False

def exec_command(line,current_pos,thread_no):	
	line = line.lstrip(" ")
	line = line.rstrip(" ")
	if line == "":
		return current_pos +1
	command = line.split(" ")
	if command[0][0] == "#":
		command.remove(command[0])

	i = 0
	while i < len(command):
		if command[i] == '':
			command.remove(command[i])
		else:
			i += 1


	if command[0] == "ADD" or command[0] == "SUB" or command[0] == "MUL" or command[0] == "DIV" or command[0] == "MOD":
		ok = arithmetic(command,thread_no)
	elif command[0] == "BGT" or command[0] == "BGE" or command[0] == "BLT" or command[0] == "BLE" or command[0] == "BEQ":
		ok = branch(command,thread_no)
	elif command[0] == "BRA":
		ok = bra(command,thread_no)
	elif command[0] == "PRN":
		ok = prn(command,thread_no)
	elif command[0] == "SLP":
		ok = slp(command,thread_no)
	elif command[0] == "SET":
		ok = setvar(command,thread_no)
	elif command[0] == "PUT":
		ok = put(command,thread_no)
	elif command[0] == "GET":
		ok = get(command,thread_no)
	elif command[0] == "DEL":
		ok = delete(command,thread_no)
	elif command[0] == "EXT":
		ok = -2
	else:
		print "Syntax error: command not found"
		ok = False

	if ok == False:
		return -1
		#close program
	elif ok == -1 or ok == True:
		return current_pos +1
		#go to next command
	else:
		return ok
		#move fd according to ok
	
def exec_file(fname,thread_no,arguments,ivals,svals,line):
	global strvals,intvals
	if ivals == None:
		intvals[thread_no] = {}
	else:
		intvals[thread_no] = ivals
	if svals == None:
		strvals[thread_no] = {}
	else:
		strvals[thread_no] = svals
	labels[thread_no] = {}
	current_pos = line
	
	if arguments != None:
		argC=1
		strvals[thread_no]["$argv0"] = globalStr.threads[thread_no][1]

		for i in arguments:
			try:
				val = int(i)
				name="$argv"+str(argC)
				argC += 1
				intvals[thread_no][name] = val
			except:
				if i[0]=="\"" and i[len(i)-1]=="\"":
					val = i[1:len(i)-1]
					name="$argv"+str(argC)
					argC += 1
					strvals[thread_no][name] = val
				else:
					print "Error: not correct val"
					del intvals[thread_no]
					del strvals[thread_no]
					del labels[thread_no]
					globalStr.threads[thread_no][2] = "exit"
					return
	
	lines = [line.rstrip('\n') for line in open(fname)]
	lines[0]=lines[0].lstrip(" ")
	lines[0]=lines[0].rstrip(" ")
	if lines[0] != "#SIMPLESCRIPT":
		
		del intvals[thread_no]
		del strvals[thread_no]
		del labels[thread_no]
		globalStr.threads[thread_no][2] = "exit"
		return
	
	lines.remove(lines[0])
	lineC=0
	
	for i in lines :
		i=i.lstrip()
		i=i.rstrip()
		if i != "":
			l = i.split(" ") 
			if l[0][0] == "#":
				labels[thread_no][l[0]] = lineC
		lineC +=1
		
	while True: 
		if globalStr.threads[thread_no][3] == True:
			globalStr.immigrateData[thread_no] = [fname,intvals[thread_no],strvals[thread_no],current_pos]
			globalStr.threads[thread_no][2] = "exit"
			return
		current_pos = exec_command(lines[current_pos],current_pos,thread_no)
		if current_pos == -2 or current_pos == -1:
			del intvals[thread_no]
			del strvals[thread_no]
			del labels[thread_no]
			globalStr.threads[thread_no][2] = "exit"
			return
		
