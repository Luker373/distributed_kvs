from flask import Flask, abort, request, redirect
import os, socket, requests, json, time, math, threading, random, itertools

app = Flask(__name__)

partitioned_nodes = []      #list of nodes currently unreachable by this node
nodes = []                  #all the nodes in the view
localNodes = []             #the nodes in the local partition
vectorClock = []            #length will be established in initialize()
myPartition = -1            #the partition # of the current node
partitionCount = -1         # represents count of every partition with nodes in them
viewSize = -1

D = {}                      #our dictionary

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

#finds partition of a given node based on position in VIEW
def findPartition(nodeInfo):
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
    if key in D:
        return json.dumps({"msg" : "success", "key" : key, "value" : D[key], "partition_id" : myPartition, "nodeInfo" : myInfo}, indent = 4, separators=(',',' : ')), 200
    return json.dumps({"msg" : "failed", "exists" : "false"}, indent = 4, separators=(',',' : ')), 200

#returns the vector clock for the partition from the called node's view
@app.route('/kvs/checkVC', methods=['GET'])
def checkVC():
    return json.dumps(vectorClock)


#finds the location of a key if it exists (mostly for debugging)
@app.route('/kvs', methods=['GET'])
def findKey():
    if request.args.get('key')==None:
        return json.dumps({"msg" : "error", "key" : "no key specified"})
    key = request.args.get('key')
    if key in D:
        return json.dumps({"msg" : "success", "key" : key, "value" : D[key][0], "timestamp" : D[key][2], "partition_id" : myPartition, "Found First At" : myInfo, "causal_payload" : json.dumps(vectorClock)}, indent = 4, separators=(',',' : ')), 200
    else:
        for node in nodes:
            if (node not in localNodes) and (node not in partitioned_nodes):
                req = requests.get('http://' + node + '/kvs/checkKey?key=' + key)
                if req.json()["msg"] == "success":
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
    if request.form.get('excludedPartition'):
        excludedPartition = request.form.get('excludedPartition')
    else:
        excludedPartition = -1

    excludedPartition = request.form.get('value')
    if(request.form.get('causal_payload')!=None and request.form.get('causal_payload')!=''):
        VC = json.loads(request.form.get('causal_payload'))
    else:
        VC = vectorClock

    #updates the vectorClock to match the payload (currently assumes VC lengths match)
    for i in range (0, int(len(VC))-1):
        if VC[i] > vectorClock[i]:
            vectorClock[i] = VC[i]

    timeStamp = time.clock() #timestamp for comparing values across healed netowrk partitions   
    if(len(keyToIns)>250):
        return json.dumps({"msg" : "error", "key" : "key too long"}, indent = 4, separators=(',',' : ')), 404
    if(valueToIns is None):
        return json.dumps({"msg" : "error", "value" : "no value specified"}, indent = 4, separators=(',',' : ')), 404
    
    if not 'keyExists' in locals():
        keyExists = json.loads(keyInKVS(keyToIns)) 
    else:
        updateLocal(keyToIns, valueToIns, VC, timeStamp)
        #pass the update to the other nodes in the partition  
        replicate(keyToIns)       
        if keyExists[0] == -1:
            keyExists[0] = myPartition
        return json.dumps({"msg" : "success", "value" : valueToIns, "partition_id" : keyExists[0], "causal_payload" : json.dumps(vectorClock), "timestamp" : timeStamp}, indent = 4, separators=(',',' : ')), 200

    if keyExists[0] == myPartition:      
        updateLocal(keyToIns, valueToIns, VC, timeStamp)
        #pass the update to the other nodes in the partition
        replicate(keyToIns) 
    #if the key exists on another partition and the key is not on a partition that is being deleted
    elif keyExists[0] != -1 and excludedPartition != keyExists[0]: 
        req = requests.put('http://' + keyExists[1] + '/kvs', data = {'key':keyToIns, 'value':valueToIns, 'causal_payload':json.dumps(VC), 'keyExists' : json.dumps(keyExists)}) #sends the request to the node that has the key already
        return json.dumps(req.json(), indent = 4, separators=(',',' : '))
    #if the key does not exist, add it to the partition with the least amount of keys
    else: 
        nodeWithLeastKeys = myInfo
        leastKeys = len(D)
        # finding a node with the least keys
        for node in nodes:
            if (node not in partitioned_nodes) and (node != myInfo) and (node not in localNodes):
                numKeyReq = requests.get('http://' + node + '/kvs/get_number_of_keys')
                if numKeyReq.json()["count"] < leastKeys:
                    nodeWithLeastKeys = node
                    leastKeys = numKeyReq.json()["count"]
        if nodeWithLeastKeys!=myInfo:
            req = requests.put('http://' + nodeWithLeastKeys + '/kvs', data = {'key':keyToIns, 'value':valueToIns, 'causal_payload': json.dumps(VC), 'keyExists' : json.dumps(keyExists)}) #sends the request to the node that has the key already
            return json.dumps(req.json(), indent = 4, separators=(',',' : '))

        #once we have reached the partition with the least keys, add it
        updateLocal(keyToIns, valueToIns, VC, timeStamp) 
        replicate(keyToIns)      
    
    if keyExists[0] == -1:
        keyExists[0] = myPartition

    return json.dumps({"msg" : "success", "value" : valueToIns, "partition_id" : keyExists[0], "causal_payload" : json.dumps(vectorClock), "timestamp" : timeStamp}, indent = 4, separators=(',',' : ')), 200

#returns the partition that the key is located in. If no partition has the key, returns -1
def keyInKVS(key):
    #partitionInfo: holds the info of the node with the key. Index 0 = partition #, Index 1 = Node. Node -1 = not found
    partitionInfo = [-1,-1] 
    if key in D:
        partitionInfo[0] = myPartition 
        partitionInfo[1] = myInfo
        return json.dumps(partitionInfo) 
    else:
        for node in nodes:
            if (node not in partitioned_nodes) and (node != myInfo) and (node not in localNodes):
                try:
                    req = requests.get('http://' + node + '/kvs/checkKey?key=' + key, timeout = 0.9)
                    temp1 = req.json()["msg"]
                    if temp1 == 'success':
                        partitionInfo[0] = req.json()["partition_id"]
                        partitionInfo[1] = node
                        return json.dumps(partitionInfo)
                except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                    partitioned_nodes.append(node)
                    return json.dumps(partitionInfo)
        #no key was found. [-1,-1] is returned
        return json.dumps(partitionInfo)

#replicates the added/changed key to other nodes in the partition                				
def replicate(key):
    #value: value[0] = value, value[1] = version#, value[2] = timestamp  
    value = D[key]
    for node in localNodes:
        if (node not in partitioned_nodes) and (node != myInfo):
            try:
                requests.put('http://' + node + '/forceput', data={'key': key, 'value': json.dumps(value),'causal_payload': json.dumps(vectorClock)}, timeout = 2)
            except(requests.Timeout, requests.ConnectionError, requests.RequestException):
                partitioned_nodes.append(node)
    return json.dumps('success'), 200

#for testing contents of nodes
@app.route('/kvsDump', methods=['GET'])
def dumpKVS():
    return json.dumps(D), 200

#updates the specified key at this node
#causal_payload = vector clock list of nodes in this partition
def updateLocal(keyToIns, valueToIns, causal_payload, timestamp):
    global D
    global vectorClock
    myIndex = localNodes.index(myInfo)
    if keyToIns in D: 
        vectorClock[myIndex] += 1
        D[keyToIns][0] = valueToIns
        D[keyToIns][1] += 1
        D[keyToIns][2] = timestamp
        return 'success'
    else:
        vectorClock[myIndex] += 1
        D[keyToIns] = [valueToIns,1,timestamp] #version 1 as it is the first change
        return 'success'
    return 'failed updateLocal()'	
	           
#tries to update a key-value pair to a more recent one
#will check if a key is present in a node, if so it will compare causal data and insert if appropriate (this is still not complete as the causal_payload part is not yet complete)    	
@app.route('/forceput', methods=['PUT'])
def forcePut():
    global D
    global vectorClock
    keyToIns = request.form.get('key')
    valueToIns = json.loads(request.form.get('value'))
    sentVC = json.loads(request.form.get('causal_payload'))

    for i in range (0, int(len(sentVC))-1):
        if sentVC[i] > vectorClock[i]:
            vectorClock[i] = sentVC[i]

    myIndex = localNodes.index(myInfo)

    if keyToIns in D:
        if valueToIns[2] > D[keyToIns][2]:
            D[keyToIns] = valueToIns
            vectorClock[localNodes.index(myInfo)]+=1
            return 'success'
    else:
        D[keyToIns] = valueToIns
        vectorClock[localNodes.index(myInfo)]+=1
        return 'success'

#to be used for adding nodes to already existing partitions
@app.route('/size',methods=['GET'])
def myPartitionSize():
    return json.dumps({"size" : len(localNodes)},  indent = 4, separators=(',',' : ')),200

@app.route('/kvs/get_partition_id', methods=['GET']) 
def returnMyPartition():
    return json.dumps({"msg" : "success", "partition_id" : myPartition}, indent = 4, separators=(',',' : ')), 200

@app.route('/partition', methods=['GET'])
def returnMyId():
    return json.dumps({"partition" : myPartition}) , 200

@app.route('/partitionCount', methods=['GET'])
def returnPartCount():
    calcPartitionSize()
    return json.dumps({"partitionCount": partitionCount})

# Ping a given address
def ping(address):
    try:
        req = requests.get('http://' + address + '/resp', timeout = 2)
        if req.status_code == 200:
            return address  
    except(requests.Timeout, requests.ConnectionError, requests.RequestException):
        nodes.remove(address)
        return None

# Ping all nodes in current view
@app.route('/pingall', methods=['GET'])
def pingAll():
    global nodes
    resps = []
    for node in nodes:
        if myInfo != node:
            respPing = ping(node)
            if respPing != None:
                resps.append(respPing)
    resps.append(myInfo)
    	
    return json.dumps(resps), 200

# Ping nodes in the same partition
@app.route('/pinglocal', methods=['GET'])
def pingLocal():
    resps = []
    for node in localNodes:
        if myInfo != node:
            respPing = ping(node)
            if respPing != None:
                resps.append(respPing)
    resps.append(myInfo)
    	
    return json.dumps(resps), 200

# Error redirect
@app.route('/error',methods=['GET'])
def errorResp():
    return json.dumps({"msg" : "error", "error" : "key value store is not available"}, indent = 4, separators=(',',' : ')), 500

# Redirect from home
@app.route('/resp' , methods=['GET'])
def resp():
    return myInfo, 200

# Root
@app.route('/' , methods=['GET'])
def homeRedirect():
    return redirect('/resp', code = 302)

# returns all nodes that belong to local partition
@app.route('/my_members', methods=['GET'])
def myPartition():
    return json.dumps(localNodes), 200

# returns nodes in view
@app.route('/my_view', methods=['GET']) 
def myView():
    return json.dumps(nodes), 200

# returns nodes in a given partition ID as seen by this node
@app.route('/kvs/get_partition_members', methods=['GET'])
def getMembers():
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

#used for testing
@app.route('/part',methods=['GET'])
def testPart():
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

# Ping each node in view periodically to see if still reachable
# TODO: update to use ping()
def heartbeat():
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

# returns all partitions with reachable members
@app.route('/kvs/get_all_partition_ids', methods=['GET'])
def getAllID():
    idList = []
    nodesSize = math.ceil(float(len(nodes))/float(viewSize))
    for x in range(1, int(nodesSize)+1):
        idList.append(x)

    return json.dumps({"msg" : "success", "partition_id_list" : idList})

# calculates the current size of the partitions
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

# default values for a node
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

# Sends an update on data to other nodes in local and global view
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

# empties dictionary and returns it
@app.route('/toDelete', methods=['GET'])
def toDelete():
    global D
    tempDict = D
    D = {}
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

# update the view of the current node	
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
                localNodes.append(address)
            if address not in nodes:
                nodes.insert(nodes.index(myInfo)+1, address)
            # update other nodes of this change
            toSend(myPartition, localNodes, nodes, viewSize, partitionCount, address)
            for node in nodes:
                if node != myInfo and node != address:
                    if node != sender:
                        try:
                            requests.put('http://' + node + '/kvs/send_update', data={'type': 'add', 'affectedNode':address, 'index':nodes.index(address), 'affectedPartition':-1}, timeout = 2)
                        except:
                            return json.dumps({"msg": 'ERR', 'node': node})
            added = True
        #find a node whose partition it can join
        else:
            for node in nodes:
                if node not in localNodes: #shouldn't need this in theory 
                    req = requests.get('http://' + node + '/size', timeout = 2)
                    temp = int(req.json()["size"])
                    if float(temp) < float(viewSize) and node != sender:
                        req2 = requests.put('http://' + node + '/kvs/view_update', data={'ip_port':address, 'type':'add', 'sender': myInfo, 'senderPart': myPartition}, timeout = 2)#need to do more testing with this
                        if req2.json()["msg"] == 'ERR':
                            return json.dumps(req2.json())
                        if req2.json()["msg"] == "success":
                            added = True
                            nodes = json.loads(requests.get('http://' + node + '/kvs/request_update', timeout=2).text)
                            calcPartitionSize()
                            return json.dumps({"msg" : "success", "partition_id" : req2.json()["partition_id"], "number_of_partitions" : partitionCount}, indent = 4, separators=(',',' : ')), 200
        if added:
            calcPartitionSize(sender, senderPart)
            tempPartCount = math.ceil(len(nodes)/viewSize)
            return json.dumps({"msg" : "success", "partition_id" : partitionCount, "number_of_partitions": tempPartCount}, indent = 4, separators=(',',' : ')), 200 
        # have to make a new partition for this key
        else:
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
    
    # handle the removal of a key from the kvs
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

                if float(partSize) < float(viewSize):
                    # check every other partition and see if they have space, starting from the front
                    for otherNode in nodes:
                        if otherNode == node:
                            continue

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
                                timeStamp = time.clock()
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

# move a node's keys from one partition to another
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

    #if this is the last node in the partition that is being moved, move keys off
    if (len(localNodes) == 1): 
        # this line may also error for the same reasons that requests.get('http://' + node + '/copyDict') could error above
        # maybe add a parameter to moveNode to fix this?
        dictToMove = toDelete() # Gets the dictionary to be moved ###############################################################################################################################
    myPartition = float(toMoveTo)
    if localRep == 0:
        return json.dumps({"ERR": myInfo, "LOCALREP = 0, toMoveTo:": toMoveTo})
    if shouldSend == True:
        localNodes = json.loads(requests.get('http://' + localRep + '/kvs/request_local_update',timeout=2).text)
    else:
        del localNodes[:]
        localNodes.append(localRep)
    localNodes.append(myInfo)

    return dictToMove

# return the local view of this node's partition
@app.route('/kvs/request_local_update', methods=['GET'])
def reqLocalUpdate():
    return json.dumps(localNodes), 200

# return the global view of this node
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

    return json.dumps(nodes), 200

# TODO: finish causal payload
@app.route('/vectorClock', methods=['GET'])
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
        vectorClock.append(0)
    return 'Success' , 200

if __name__ == '__main__':
    initialize()
    threading.Timer(3,heartbeat).start() #may need to alter the frequency of calls to heartbeat() (probably will need to be less)
    app.run(host=myIP, port=(int(myPort)))