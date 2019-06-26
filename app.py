from flask import Flask, abort, request, redirect
import os, socket, requests, json, time, math, threading, random, itertools

app = Flask(__name__)

partitioned_nodes = [] #list of nodes currently unreachable by this node
nodes = [] #all the nodes in the view
localNodes = [] #the nodes in the local partition
vectorClock = [] #length will be established in initialize()
myPartition = -1 #the partition # of the current node
partitionCount = -1         # represents count of every partition with nodes in them
viewSize = -1

D = {} #our dictionary

myInfo = os.getenv('ip_port')
myIP,myPort = myInfo.split(':')

if(os.getenv('VIEW') != None):
    viewNodes = os.getenv('VIEW')
    nullView = False
else:
    viewNodes = myInfo
    nullView = True

for dataNode in viewNodes.split(','):
    nodes.append(dataNode)

if(os.getenv('K') != None):
    viewSize = float(os.getenv('K'))
    partitionCount = math.ceil((len(nodes))/viewSize)
else:
    viewSize = -1

def findPartition(nodeInfo):#finds partition of a given node based on position in VIEW
    partition = 1
    count = 1
    if len(nodes) == 1:
        return -1
    for node in nodes:
        if node == nodeInfo:
            return partition
        if count == viewSize:
            partition+=1
            count = 1
        else:
            count+=1
    return -1

#returns the number of keys in the dictionary
@app.route('/kvs/get_number_of_keys', methods=['GET'])
def getNumKeys():
    return json.dumps({"count" : len(D)}, indent = 4, separators=(',',' : ')), 200

#checks to see if the specified key exists
@app.route('/kvs/checkKey', methods=['GET'])
def checkKey():
    key = request.args.get('key')
    if key in D: #if the key is found, return success
        return json.dumps({"msg" : "success", "key" : key, "value" : D[key], "partition_id" : myPartition, "nodeInfo" : myInfo}, indent = 4, separators=(',',' : ')), 200
    return json.dumps({"msg" : "failed", "exists" : "false"}, indent = 4, separators=(',',' : ')), 200

#returns the vector clock for the partition from the called node's view
@app.route('/kvs/checkVC', methods=['GET'])
def checkVC():
    return json.dumps(vectorClock)



#finds the location of a key if it exists (mostly for debugging)
@app.route('/kvs', methods=['GET'])
def findKey():
    if request.args.get('key')==None: #errors if no key is specified
        return json.dumps({"msg" : "error", "key" : "no key specified"})
    key = request.args.get('key')
    if key in D: #the key was on this node
        return json.dumps({"msg" : "success", "key" : key, "value" : D[key][0], "timestamp" : D[key][2], "partition_id" : myPartition, "Found First At" : myInfo, "causal_payload" : json.dumps(vectorClock)}, indent = 4, separators=(',',' : ')), 200
    else:
        for node in nodes: #search other nodes
            if (node not in localNodes) and (node not in partitioned_nodes):
                req = requests.get('http://' + node + '/kvs/checkKey?key=' + key)
                if req.json()["msg"] == "success": #if the message was found
                    return json.dumps({"msg" : "success", "key" : req.json()["key"], "value" : req.json()["value"][0], "timestamp" : req.json()["value"][2], "partition_id" : req.json()["partition_id"], "Found First At" : req.json()["nodeInfo"], "causal_payload" : json.dumps(vectorClock)}, indent = 4, separators=(',',' : ')), 200
    return json.dumps({"msg" : "failed", "key" : "key not found"}, indent = 4, separators=(',',' : ')), 200

#handles adding and deleting a key from the KVS system    
@app.route('/kvs', methods=['POST','PUT'])
def kvsPosPut():
    global D
    global vectorClock
    VC = [] #the vector clock for the current node's view

    #used to keep track of the last call to kvs to prevent loops in calls
    if request.form.get('keyExists')!=None:
        keyExists = json.loads(request.form.get('keyExists'))

    keyToIns = request.form.get('key')
    valueToIns = request.form.get('value')
    #used when moving a dictionary off a partition shutting down
    if request.form.get('excludedPartition'): #if an excluded partition is included, use it
        excludedPartition = request.form.get('excludedPartition')
    else:
        excludedPartition = -1 #no partition is excluded

    excludedPartition = request.form.get('value')
    if(request.form.get('causal_payload')!=None and request.form.get('causal_payload')!=''): #gets causal payload if specified.
        VC = json.loads(request.form.get('causal_payload'))
    else: #otherwise, use vectorClock of current node
        VC = vectorClock

    #updates the vectorClock to match the payload (currently assumes VC lengths match)
    for i in range (0, int(len(VC))-1):
        if VC[i] > vectorClock[i]: #if the payload has a newer version, update this one locally first
            vectorClock[i] = VC[i] #updates the vectorClock to the higher value found in the payload

    timeStamp = time.clock() #timestamp for comparing values across healed netowrk partitions   
    if(len(keyToIns)>250): #Error if key is too long
        return json.dumps({"msg" : "error", "key" : "key too long"}, indent = 4, separators=(',',' : ')), 404
    if(valueToIns is None): #error if no value specified
        return json.dumps({"msg" : "error", "value" : "no value specified"}, indent = 4, separators=(',',' : ')), 404
    
    if not 'keyExists' in locals(): #loop only if we havent found the key yet (works by checking to see if keyExists exists)
        keyExists = json.loads(keyInKVS(keyToIns)) #gets the partition that the key is in. returns '-1' if the key does not exist [0] = partition#, [1] = nodeInfo
    else: #we have reached the correct node, update it
        updateLocal(keyToIns, valueToIns, VC, timeStamp) #update it locally
        replicate(keyToIns) #pass the update to the other nodes in the partition        
        if keyExists[0] == -1: #assigns the correct partition if a new key was created
            keyExists[0] = myPartition
        return json.dumps({"msg" : "success", "value" : valueToIns, "partition_id" : keyExists[0], "causal_payload" : json.dumps(vectorClock), "timestamp" : timeStamp}, indent = 4, separators=(',',' : ')), 200

    if keyExists[0] == myPartition: #if the key exists on this current partition       
        updateLocal(keyToIns, valueToIns, VC, timeStamp) #update it locally
        replicate(keyToIns) #pass the update to the other nodes in the partition
    elif keyExists[0] != -1 and excludedPartition != keyExists[0]: #if the key exists on another partition and the key is not on a partition that is being deleted
        req = requests.put('http://' + keyExists[1] + '/kvs', data = {'key':keyToIns, 'value':valueToIns, 'causal_payload':json.dumps(VC), 'keyExists' : json.dumps(keyExists)}) #sends the request to the node that has the key already
        return json.dumps(req.json(), indent = 4, separators=(',',' : '))
    else: #if the key does not exist, add it to the partition with the least amount of keys
        nodeWithLeastKeys = myInfo #sets the node with the least keys to be this node 1st
        leastKeys = len(D)

        #handles finding a node with the least keys
        for node in nodes: #checks all nodes not in this partition and can be found
            if (node not in partitioned_nodes) and (node != myInfo) and (node not in localNodes):
                numKeyReq = requests.get('http://' + node + '/kvs/get_number_of_keys')
                if numKeyReq.json()["count"] < leastKeys: #if a new smallest key number is found
                    nodeWithLeastKeys = node
                    leastKeys = numKeyReq.json()["count"]
        if nodeWithLeastKeys!=myInfo: #if the partition with the least keys isnt this partition
            req = requests.put('http://' + nodeWithLeastKeys + '/kvs', data = {'key':keyToIns, 'value':valueToIns, 'causal_payload': json.dumps(VC), 'keyExists' : json.dumps(keyExists)}) #sends the request to the node that has the key already
            return json.dumps(req.json(), indent = 4, separators=(',',' : '))

        #once we have reached the partition with the least keys, add it
        updateLocal(keyToIns, valueToIns, VC, timeStamp) #update it locally
        replicate(keyToIns) #pass the update to the other nodes in the partition       
    
    if keyExists[0] == -1: #assigns the correct partition if a new key was created
        keyExists[0] = myPartition

    return json.dumps({"msg" : "success", "value" : valueToIns, "partition_id" : keyExists[0], "causal_payload" : json.dumps(vectorClock), "timestamp" : timeStamp}, indent = 4, separators=(',',' : ')), 200

#returns the partition that the key is located in. If no partition has the key, returns -1
def keyInKVS(key):
    partitionInfo = [-1,-1] #holds the info of the node with the key. Index 0 = partition #, Index 1 = Node. Note -1 = not found
    if key in D: #is the key in this partition?
        # if yes, return this partition
        partitionInfo[0] = myPartition #gets this node's partition
        partitionInfo[1] = myInfo #gets this node's info
        return json.dumps(partitionInfo) 
    else: #otherwise search the other nodes for the key
        for node in nodes: #checks all nodes not in this partition and can be found
            if (node not in partitioned_nodes) and (node != myInfo) and (node not in localNodes):
                try:
                    #check for the key in each node
                    req = requests.get('http://' + node + '/kvs/checkKey?key=' + key, timeout = 0.9)
                    temp1 = req.json()["msg"]
                    if temp1 == 'success': #if the key was found in a different partition
                        partitionInfo[0] = req.json()["partition_id"]
                        partitionInfo[1] = node
                        return json.dumps(partitionInfo) #return the partition that the key is a part of
                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                    partitioned_nodes.append(node)
                    return json.dumps(partitionInfo)
        return json.dumps(partitionInfo) #no key was found. [-1,-1] is returned

#replicates the added/changed key to other nodes in the partition                				
def replicate(key):
    value = D[key] #value[0] = value, value[1] = version#, value[2] = timestamp  
    for node in localNodes:
        if (node not in partitioned_nodes) and (node != myInfo): #for other nodes in partition
            try:
                #update key change
                requests.put('http://' + node + '/forceput', data={'key': key, 'value': json.dumps(value),'causal_payload': json.dumps(vectorClock)}, timeout = 2)
            except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                partitioned_nodes.append(node)
    return json.dumps('success'), 200

@app.route('/kvsDump', methods=['GET'])
def dumpKVS():#for testing contents of node
    return json.dumps(D), 200

#updates the specified key at this node
#causal_payload = vector clock list of nodes in this partition
def updateLocal(keyToIns, valueToIns, causal_payload, timestamp):
    global D
    global vectorClock
    myIndex = localNodes.index(myInfo) # gets the index location of this node in the local view to update the vector clock
    if keyToIns in D: #if the key is alread in this node's dictionary    (update it)    
        vectorClock[myIndex] += 1
        D[keyToIns][0] = valueToIns #updates the value to the new value
        D[keyToIns][1] += 1 #increments the version # of the value
        D[keyToIns][2] = timestamp # updates to new timestamp
        return 'success'
    else:#if the key is not in the node, will simply add it
        vectorClock[myIndex] += 1
        D[keyToIns] = [valueToIns,1,timestamp] #version 1 as it is the first change
        return 'success'
    return 'failed updateLocal()'	
	           
#tries to update a key-value pair to a more recent one    	
@app.route('/forceput', methods=['PUT'])
def forcePut():#will check if a key is present in a node, if so it will compare causal data and insert if appropriate (this is still not complete as the causal_payload part of the assignment is not yet complete)
    global D
    global vectorClock
    keyToIns = request.form.get('key')
    valueToIns = json.loads(request.form.get('value')) #the value list (value,version,timestamp)
    sentVC = json.loads(request.form.get('causal_payload'))

    #updates the vectorClock to match the payload (currently assumes VC lengths match)
    for i in range (0, int(len(sentVC))-1):
        if sentVC[i] > vectorClock[i]: #if the payload has a newer version, update this one locally first
            vectorClock[i] = sentVC[i] #updates the vectorClock to the higher value found in the payload

    myIndex = localNodes.index(myInfo)

    if keyToIns in D: #if the key already exists at this node
        if valueToIns[2] > D[keyToIns][2]: #if the sent key has a more recent timestamp
            D[keyToIns] = valueToIns #updates the value to the more recent one
            vectorClock[localNodes.index(myInfo)]+=1 # increments the local vector clock
            return 'success'
    else: #if the key does not already exist
        D[keyToIns] = valueToIns #updates the value to the more recent one
        vectorClock[localNodes.index(myInfo)]+=1 # increments the local vector clock
        return 'success'

#to be used for adding nodes to already existing partitions
@app.route('/size',methods=['GET'])
def myPartitionSize():
    return json.dumps({"size" : len(localNodes)},  indent = 4, separators=(',',' : ')),200

@app.route('/kvs/get_partition_id', methods=['GET']) 
def returnMyPartition(): #required method, may need extra comma after partition # to match specs
    return json.dumps({"msg" : "success", "partition_id" : myPartition}, indent = 4, separators=(',',' : ')), 200
# seems like a duplicate of the above method?
@app.route('/partition', methods=['GET'])
def returnMyId():
    return json.dumps({"partition" : myPartition}) , 200

@app.route('/partitionCount', methods=['GET'])
def returnPartCount():
    calcPartitionSize()
    return json.dumps({"partitionCount": partitionCount})

def ping(address):
    try:
        req = requests.get('http://' + address + '/resp', timeout = 2)
        if req.status_code == 200:
            return address  
    except(requests.Timeout, requests.ConnectionError, requests.RequestException):
        nodes.remove(address)
        return None 	#504 errors are used as Gateway Timeout errors
@app.route('/pingall', methods=['GET'])
def pingAll():
    global nodes
    resps = []
    for node in nodes:
        if myInfo != node:               #removed to allow all nodes to see themselves
            respPing = ping(node)
            if respPing != None:
                resps.append(respPing)
    resps.append(myInfo)
    	
    return json.dumps(resps), 200

@app.route('/pinglocal', methods=['GET']) #similar to pingAll but will only ping nodes in the same partition
def pingLocal():
    resps = []
    for node in localNodes:
        if myInfo != node:               #removed to allow all nodes to see themselves
            respPing = ping(node)
            if respPing != None:
                resps.append(respPing)
    resps.append(myInfo)
    	
    return json.dumps(resps), 200

@app.route('/error',methods=['GET'])
def errorResp():#to be used for errors as per project spec. I have not set up any redirects to this as of yet.
    return json.dumps({"msg" : "error", "error" : "key value store is not available"}, indent = 4, separators=(',',' : ')), 500

@app.route('/resp' , methods=['GET'])
def resp():
    return myInfo, 200

@app.route('/' , methods=['GET'])
def homeRedirect():
    return redirect('/resp', code = 302)

@app.route('/my_members', methods=['GET'])#returns all nodes that belong to local partition
def myPartition():
    return json.dumps(localNodes), 200

@app.route('/my_view', methods=['GET']) #returns nodes
def myView():
    return json.dumps(nodes), 200

@app.route('/kvs/get_partition_members', methods=['GET'])
def getMembers():#working, give it a partition # and it will give you the members
    id = request.args.get('partition_id')
    if (id != None):
        out = []
        partition = int(id)
        if myPartition == partition:
            for node in localNodes:
                out.append(node)
            return json.dumps({"msg" : "success", "partition_members" : out}, indent = 4, separators=(',',' : ')), 200
        else:
            inPartition = []
            for node in nodes:
                if node not in localNodes:
                    req = requests.get('http://' + node + '/partition', timeout = 2)
                    temp = int(req.json()["partition"])
                    if temp == partition:
                        out.append(node)
            return json.dumps({"msg" : "success", "partition_members" : out}, indent = 4, separators=(',',' : ')), 200
    return id, 200
@app.route('/part',methods=['GET'])
def testPart():#used for testing
    out = ','.join(partitioned_nodes)
    return json.dumps({"Partitioned Nodes: " : out}, indent = 4, separators=(',',' : ')), 200

def sync(nodeID):
    dest = nodeID
    for key in D:
        value = D[key]
        try:
            req2 = requests.put('http://' + dest + '/forceput', data={'key': key, 'value': json.dumps(value),'causal_payload': json.dumps(vectorClock)}, timeout = 2)
        except(requests.Timeout, requests.ConnectionError, requests.RequestException):
            partitioned_nodes.append(node)
            return json.dumps('node was partitioned again'), 500
    return json.dumps('success!'), 200
def heartbeat():#will identify partitioned nodes
    while True:
        for node in nodes:
            if myInfo != node:
                global partitioned_nodes
                try:
                    resp = requests.get('http://'+ node + '/resp', timeout = 1)
                    if node in partitioned_nodes:
                        partitioned_nodes.remove(node)
                        if node in localNodes:
                            sync(node)
                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                    if node not in partitioned_nodes:
                        partitioned_nodes.append(node) 
        time.sleep(1.5)					

@app.route('/kvs/get_all_partition_ids', methods=['GET'])
def getAllID():#returns all partitions with reachable members
    idList = []
    nodesSize = math.ceil(float(len(nodes))/float(viewSize))
    for x in range(1, int(nodesSize)+1):
        idList.append(x)

    return json.dumps({"msg" : "success", "partition_id_list" : idList})
    # The below code was initially used when assuming that partitions that were 
    #  idList.append(str(myPartition))
   # for node in nodes:
    #    if node not in localNodes:
     #       req = requests.get('http://' + node + '/partition', timeout = 2)
      #      temp = str(req.json()["partition"])
       #     if temp not in idList:
        #        idList.append(temp)
    #idList = sorted(idList)
    #return json.dumps({"msg" : "success", "partition_id_list" : idList}, indent = 4, separators=(',',' : ')), 200
    

def calcPartitionSize(sender=0, senderPart=0):
    global partitionCount
    #global myPartition
    unreachableNodes = []
    temp1 = 0
    temp2 = 0
    if len(nodes) == 1:
        partitionCount = 0
        return

    for node in nodes:
        if node == sender:
            if float(senderPart) > float(temp1):
                temp1 = float(senderPart)
                temp2 += 1
                continue

        if node == myInfo:
            if float(myPartition) > float(temp1):
                temp1 = float(myPartition)
                temp2 += 1
        else:
            try:
                tempPart = float(requests.get('http://' + node + '/partition', timeout=2).json()["partition"])
                if float(tempPart) > float(temp1):
                    temp1 = tempPart
                    temp2 += 1
            except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                unreachableNodes.append(node)
    partitionCount = temp2

@app.route('/initNode', methods=['PUT'])
def initNode():
    # these seemed to mess with /initNode and the node that recieved /initNode had no data
    global myPartition
    global localNodes
    global partitionCount
    global viewSize
    global nodes
    

    myPartition = float(request.form.get('partition_id'))
 #   localView = request.form.get('local_nodes')
 #   allNodes = request.form.get('view')
    viewSize = float(request.form.get('K_Count'))
    partitionCount = float(request.form.get('p_count'))

    if myInfo in nodes:
        nodes.remove(myInfo)
    if myInfo not in localNodes:
        localNodes.append(myInfo)

    return json.dumps('Success'), 200

def toSend(partition_id, local_nodes, view, K_Count, p_count, address):
    try:
        req = requests.put('http://' + address + '/initNode', data={'partition_id':partition_id, 'local_nodes': local_nodes, 'view': view, 'K_Count' : K_Count, 'p_count' : p_count} ,timeout = 2)
        if view != None:
            for node in view:
                requests.put('http://' + address + '/kvs/send_update', data={'type': 'add', 'affectedNode':node, 'index':view.index(node), 'affectedPartition':-1})
        if local_nodes != None:
            for local in local_nodes:
                    requests.put('http://' + address + '/kvs/send_update', data={'type': 'add', 'affectedNode':local, 'index':local_nodes.index(local), 'affectedPartition':partition_id})
        else:
            return json.dumps("ERR 2")
        return local_nodes
        return json.dumps({'partition_id': partition_id, 'localNodes': local_nodes}), 200
        #return json.dumps(address), 200
    except(requests.Timeout, requests.ConnectionError, requests.RequestException):
        return None

#empties dictionary and returns it
@app.route('/toDelete', methods=['GET'])
def toDelete():
    global D
    tempDict = D
    D = {} #empties the dictionary when done
    return json.dumps(tempDict)

#returns the dictionary of the node (used when a new node needs to sync back up)
@app.route('/kvs/getDict', methods=['GET'])
def getDict():
    return json.dumps(D)

#copies a dictionary from another node in the partition
@app.route('/copyDict', methods=['GET'])        
def copyDict():
    global D
    sentFrom = 0
    if request.args.get('sentFrom')!=None:
        sentFrom = request.args.get('sentFrom')
        dictToCopy = request.args.get('dict')

    for node in localNodes:
        if node!=myInfo and sentFrom!=node: 
            req = requests.get("http://" + node + "/kvs/getDict", timeout = 2)
            tempDict = req.json()
            D = tempDict
            return json.dumps(D)
        elif node!=myInfo and sentFrom==node: #if a request loop would have been found
            D = dictToCopy #add the passed dictionary instead
            return json.dumps(D)
    return json.dumps("nothing found to copy from")		
@app.route('/kvs/view_update', methods=['PUT'])
def viewUpdate():
    global partitionCount
    global nodes
    global viewSize
    unreachableNodes = []           #represents the nodes that were unable to have their view updated due to partition
    added = False
    address = request.form.get('ip_port')
    sender = request.form.get('sender')
    senderPart = request.form.get('senderPart')
    if sender is None:
        sender = 0
    if senderPart is None:
        senderPart = -1
    calcPartitionSize(sender, senderPart)
    if request.form.get('type') == 'add':
        #can the new node join the partition of the node it was sent to?
        if float(len(localNodes)) < float(viewSize):
            if address not in localNodes:
                localNodes.append(address) # adds address to this partition
            if address not in nodes:
                nodes.insert(nodes.index(myInfo)+1, address) #adds address to view of all nodes
            toSend(myPartition, localNodes, nodes, viewSize, partitionCount, address)
            for node in nodes:
                if node != myInfo and node != address:
                    if node != sender:
                        try:
                            requests.put('http://' + node + '/kvs/send_update', data={'type': 'add', 'affectedNode':address, 'index':nodes.index(address), 'affectedPartition':-1}, timeout = 2)
                        except:
                            return json.dumps({"msg": 'ERR', 'node': node})
                        #except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                           # unreachableNodes.append(node)
            added = True
        else:#find a node whose partition it can join
            for node in nodes:
                if node not in localNodes:#shouldn't need this in theory but I added it just in case
                    req = requests.get('http://' + node + '/size', timeout = 2)
                    temp = int(req.json()["size"])
                    if float(temp) < float(viewSize) and node != sender:
                        req2 = requests.put('http://' + node + '/kvs/view_update', data={'ip_port':address, 'type':'add', 'sender': myInfo, 'senderPart': myPartition}, timeout = 2)#need to do more testing with this
                        if req2.json()["msg"] == 'ERR':
                            return json.dumps(req2.json())
                        # this prevents sending a put add request to every partition with temp < viewSize
                        if req2.json()["msg"] == "success":
                            added = True
                            nodes = json.loads(requests.get('http://' + node + '/kvs/request_update', timeout=2).text)
                            calcPartitionSize()
                            return json.dumps({"msg" : "success", "partition_id" : req2.json()["partition_id"], "number_of_partitions" : partitionCount}, indent = 4, separators=(',',' : ')), 200
        if added:
            calcPartitionSize(sender, senderPart)
            tempPartCount = math.ceil(len(nodes)/viewSize)
            return json.dumps({"msg" : "success", "partition_id" : partitionCount, "number_of_partitions": tempPartCount}, indent = 4, separators=(',',' : ')), 200 
            #return json.dumps({"msg" : "success", "partition_id" : myPartition, "number_of_partitions": partitionCount}, indent = 4, separators=(',',' : ')), 200
        else:
            #have to make a new partition for this case.
            nodes.append(address)
            toSend(partitionCount+1, None, nodes, viewSize, partitionCount, address)
            calcPartitionSize()
            for node in nodes:
                if node != myInfo:
                    if node != address:
                        try:
                            requests.put('http://' + node + '/kvs/send_update', data={'type': 'add', 'affectedNode':address, 'index':nodes.index(address), 'affectedPartition':partitionCount}, timeout = 2)
                        except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                            unreachableNodes.append(node)
            return json.dumps({"msg" : "success", "partition_id" : partitionCount, "number_of_partitions": partitionCount}, indent = 4, separators=(',',' : ')), 200
    
    if request.form.get('type') == 'remove':
        #TODO: sync all data
        # 1(xxo) 2(xxo) 3(xxo)
        # 1(xxo) 2(xoo) 3(xxo)
        try:
            tempAffPart = requests.get('http://' + address + '/kvs/get_partition_id', timeout=2).json()['partition_id']
        except(requests.Timeout, requests.ConnectionError, requests.RequestException):
            tempAffPart = -1
        for node in nodes:
            if node != myInfo:
                try:
                    requests.put('http://' + node + '/kvs/send_update', data={'type': 'remove', 'affectedNode':address, 'index': None, 'affectedPartition':tempAffPart}, timeout = 2)
                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                    unreachableNodes.append(node)                 
        nodes.remove(address)
        if address in localNodes:
            localNodes.remove(address)
        calcPartitionSize()
        idealPartNum = math.ceil(float(len(nodes))/float(viewSize))
        #return json.dumps({"idealPartNum": idealPartNum, "partitionCount": partitionCount})
        while float(idealPartNum) < float(partitionCount):
            idealPartNum = math.ceil(float(len(nodes))/float(viewSize))
            calcPartitionSize(sender, senderPart)
            # check each node's partition size starting from the back
            for node in reversed(nodes):
                if node != myInfo:
                    try:
                        req = requests.get('http://' + node + '/size', timeout=2)
                    except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                        unreachableNodes.append(node)
                    try:
                        req3 = requests.get('http://' + node + '/partition', timeout=2)
                    except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                        unreachableNodes.append(node)
                    partSize = req.json()["size"]
                    partNum = req3.json()["partition"]
                else:
                    partSize = len(localNodes)
                    partNum = myPartition
                # if the size of the currently checked node's partition is less than viewSize,

                if float(partSize) < float(viewSize):
                    # check every other partition and see if they have space, starting from the front
                    for otherNode in nodes:
                        # make sure node to check for space is not node being moved
                        if otherNode == node:
                            continue
                        # get the size of otherNode's partition

                        if otherNode == myInfo:
                            partSizeOther = len(localNodes)
                            partNumOther = myPartition
                        else:
                            try:
                                req2 = requests.get('http://' + otherNode + '/size', timeout=2)
                            except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                                unreachableNodes.append(node)
                            try:
                                req4 = requests.get('http://' + otherNode + '/partition', timeout=2)
                            except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                                unreachableNodes.append(node)
                            partSizeOther = req2.json()["size"]
                            partNumOther = req4.json()["partition"]

                        # see if otherNode's partition has space for node 
                        if float(partSizeOther) < float(viewSize) and partNumOther != partNum:
                            dictToMove = {}
                            if node == myInfo:
                                dictToMove = json.loads(moveNode(myInfo, partNumOther, otherNode))
                                # -------------------------------------------------------------------------------------- #
                                copyDict()
                                # -------------------------------------------------------------------------------------- #
                            else:
                                shouldSend = True
                                if otherNode == myInfo:
                                    if float(len(localNodes)) > float(1):
                                        for pickLocal in localNodes:
                                            if pickLocal != myInfo:
                                                otherNode = pickLocal
                                    else:
                                        shouldSend = False
                                dictToMove = (requests.put('http://' + node + '/kvs/move_node', data={'toMoveTo': partNumOther, 'localRep': otherNode, 'shouldSend': shouldSend, 'sentFrom' : myInfo}, timeout=2)).json()
                            
                                # -------------------------------------------------------------------------------------- #
                                # this line will error in the following situation
                                # part 1:(8081, 8082) part 2:(, 8084), part 3: (8085)
                                # ...99.100:8084/kvs/view_update -d "ip_port=10.0.0.23:8080&type=remove"
                                tempDict = getDict() #gets the local dict
                                requests.get('http://' + node + '/copyDict?sentFrom=' + myInfo + '&dict=' + tempDict) #retrieves the new dictionary from another node in the new partition ###############################################################################################################################
                                # when request is called by 8084, it has 8085 do copyDict(), which sends req to 8084, this causes broken pipe.
                                # I suggest adding two args to the above request, DNSinfo = myInfo and backupDict = myDict
                                # then, in copyDict(), 
                                #               for node in localNodes:
                                #                   if request.args.get('DNSinfo') == node:
                                #                       D = myDict
                                # though I'm not sure if sending a dictionary this way is viable, which is why I'm letting you guys do it ;) 
                                # -------------------------------------------------------------------------------------- #
                            # update all other nodes of affected partitions of the change
                            for bystander in nodes:
                                if bystander == node:
                                    continue
                                if bystander == myInfo:
                                    if node in localNodes:
                                        localNodes.remove(node)
                                    if partNumOther == myPartition:
                                        localNodes.append(node)
                                    continue
                                try:
                                    req7 = requests.put('http://' + bystander + '/kvs/send_update', data={'type': 'remove', 'affectedNode': node, 'affectedPartition': partNum, 'soft': True},timeout=2)
                                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                                    unreachableNodes.append(node)
                                try:
                                    req7 = requests.put('http://' + bystander + '/kvs/send_update', data={'type': 'add', 'affectedNode': node, 'affectedPartition': partNumOther},timeout=2)
                                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                                    unreachableNodes.append(node)
                            for key in dictToMove:
                                timeStamp = time.clock() #timestamp for comparing values across healed netowrk partitions
                                updateLocal(key, dictToMove[key], vectorClock, timeStamp)
                                replicate(key)
                            break
            idealPartNum = math.ceil(float(len(nodes))/float(viewSize))
            calcPartitionSize(sender, senderPart)
        
        if address == myInfo:
            nodes.remove(myInfo)
            del localNodes[:]
            del nodes[:]
            partitionCount = 0
            calcPartitionSize()
            return json.dumps({'msg': "success", "number_of_partitions": partitionCount}, indent = 4, separators=(',',' : ')), 200
        calcPartitionSize()
        return json.dumps({'msg': "success", "number_of_partitions": partitionCount}, indent = 4, separators=(',',' : ')), 200

    return json.dumps({"msg" : "ERROR", "partition_id" : myPartition, "number_of_partitions": partitionCount}, indent = 4, separators=(',',' : ')), 200

@app.route('/kvs/move_node', methods=['PUT'])
def moveNode(nodeToExclude=0,toMoveTo=0, localRep=0):
    global partitionCount
    global myPartition
    global localNodes
    if request.form.get('sentFrom') != None:
        nodeToExclude = request.form.get('sentFrom')
    if request.form.get('toMoveTo') != None:
        toMoveTo = request.form.get('toMoveTo')
    if request.form.get('localRep') != None:
        localRep = request.form.get('localRep')
    shouldSend = request.form.get('shouldSend')
    if (len(localNodes) == 1): #if this is the last node in the partition that is being moved, move keys off
        # this line may also error for the same reasons that requests.get('http://' + node + '/copyDict') could error above
        # maybe add a parameter to moveNode to fix this?
        dictToMove = toDelete() # Gets the dictionary to be moved ###############################################################################################################################
    myPartition = float(toMoveTo)
    if localRep == 0:
        return json.dumps({"ERR": myInfo, "LOCALREP = 0, toMoveTo:": toMoveTo})
    # nodes = json.loads(requests.get('http://' + node + '/kvs/request_update', timeout=2).text)
    if shouldSend == True:
        localNodes = json.loads(requests.get('http://' + localRep + '/kvs/request_local_update',timeout=2).text)
    else:
        del localNodes[:]
        localNodes.append(localRep)
    localNodes.append(myInfo)

    return dictToMove


@app.route('/kvs/request_local_update', methods=['GET'])
def reqLocalUpdate():
    return json.dumps(localNodes), 200

@app.route('/kvs/request_update', methods=['GET'])
def reqUpdate():
    return json.dumps(nodes), 200

# used to update view of nodes that aren't directly affected by viewUpdate()
@app.route('/kvs/send_update', methods=['PUT'])
def sendUpdate():
    global partitionCount
    global myPartition 
    affectedNode = request.form.get('affectedNode')
    affectedPartition = request.form.get('affectedPartition')
    typeS = request.form.get('type')
    soft = None
    if request.form.get('soft') is None:
        soft = False
    elif request.form.get('soft') == True:
        soft = True

    if typeS == 'add':
        if float(affectedPartition) == float(myPartition):
            if affectedNode not in localNodes:
                localNodes.append(affectedNode)
        if affectedNode not in nodes:
            nodes.insert(int(request.form.get('index')), affectedNode)

    if typeS == 'remove':
        if affectedNode == myInfo:
            del nodes[:]
            del localNodes[:]
            nodes.append(myInfo)
            localNodes.append(myInfo)
        if affectedNode in nodes:
            if soft == False:
                nodes.remove(affectedNode)
        if affectedNode in localNodes:
            localNodes.remove(affectedNode)
#        calcPartitionSize()

    return json.dumps(nodes), 200

@app.route('/vectorClock', methods=['GET']) #a TEMP route to debug the vectorClock
def getCausalPayload():
    global vectorClock
    test = json.dumps(vectorClock)
    test2 = json.loads(test)
    return json.dumps(test2)

def initialize():
    global myPartition
    global localNodes
    myPartition = findPartition(myInfo)
    if len(nodes) == 0:
        return 'NewNode' , 200
    for node in nodes:
        if myPartition == findPartition(node):
            if node not in localNodes:
                localNodes.append(node)     
    for x in range(1, int(viewSize+1)):
        vectorClock.append(0) #initializes the payload to be the length 
    return 'Success' , 200

if __name__ == '__main__':
    initialize()
    threading.Timer(3,heartbeat).start() #may need to alter the frequency of calls to heartbeat() (probably will need to be less)
    app.run(host=myIP, port=(int(myPort)))