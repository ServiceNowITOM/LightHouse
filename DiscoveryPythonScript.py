import time
import pytz
import json
import boto3
from dateutil.tz import tzutc
import datetime
import calendar
from ast import literal_eval
import base64
import pymongo
import sys
import datetime
import requests



##Create a MongoDB client and open connection to Amazon DocumentDB
client = pymongo.MongoClient('MongoDB ConnectionString')

##DB to use
def db_to_use(DB_Name, client):
	myDB = client[DB_Name]
	return myDB

##Collection to use
def Collection_to_use(Collection_Name, database):
	myCol = database[Collection_Name]
	return myCol

##Update or Add Record  
def Update_Record(Json_Query,Json_Update,collection):
	Json_Update['last_modified'] = datetime.datetime.utcnow()
	myObj = {}
	myObj = {"$set": Json_Update}
	myjson = json.dumps(myObj, default = myconverter)
	myjson = json.loads(myjson)
	results = collection.update_one(Json_Query,myObj,True)
	return results
	
def Update_Record_addsysID(Json_Query,Json_Update,collection):
	#Json_Update['last_modified'] = datetime.datetime.utcnow()
	myObj = {}
	myObj = {"$set": Json_Update}
	myjson = json.dumps(myObj, default = myconverter)
	myjson = json.loads(myjson)
	results = collection.update_one(Json_Query,myObj,True)
	return results

##Query and print Record
def Query_Collection(Json_Query, collection):
	docs = collection.find(Json_Query)
	return docs
	
def Print_All(collection):
	docs = collection.find()
	for doc in docs:
		doc.pop('_id', None)
		doc.pop('last_modified', None)
		print(doc)
		print("DOC END ###############")
		
		
def Query_Collection_NoLM(Json_Query, collection):
	global sys_id
	global datemod
	docs = collection.find(Json_Query)
	for doc in docs:
		doc.pop('_id', None)
		datemod = doc.pop('last_modified', None)
		doc.pop('id',None)
		sys_id = doc.pop('sys_id',None)
		return doc
		
def Get_Sys_ID(doc):
	return doc.pop('sys_id',None)


def myconverter(o):
	if isinstance(o, datetime.datetime):
		o.replace(tzinfo=None)
		return o.timestamp()

       
def myconverternew(o):
	if isinstance(o, datetime.datetime):
		o.replace(tzinfo=None)
		return return o.timestamp()
		
def update_sn(uuid,i_state,ip,modelnumber,name,tags,sysid):
    url = 'https://<SN INSTANCE NAME>.service-now.com/api/now/table/cmdb_ci_vm_instance/'+sysid
    user = 'USERNAME'
    pwd = 'PASSWORD'
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    response = requests.patch(url, auth=(user, pwd), headers=headers ,data='{"object_id": "' + uuid + '", "state": "' +  i_state + '", "ip_address": "' + ip + '", "model_number": "' + modelnumber + '", "u_tags": "' + tags + '", "name": "' + name + '"}')
    data = response.json()
    #print("Update")
    #print(data)
    myresult = data.pop('result',None)
    if myresult:
    	sysid = myresult.pop('sys_id',None)
    	thereturn = sysid
    else:
    	thereturn = None 
    return thereturn

def write_sn(uuid,i_state,ip,modelnumber,name,tags):
    url = 'https://<SN INSTANCE NAME>.service-now.com/api/now/table/cmdb_ci_vm_instance'
    user = 'USERNAME'
    pwd = 'PASSWORD'
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    response = requests.post(url, auth=(user, pwd), headers=headers ,data='{"object_id": "' + uuid + '", "state": "' +  i_state + '", "ip_address": "' + ip + '", "model_number": "' + modelnumber + '", "u_tags": "' + tags + '", "name": "' + name + '"}')
    data = response.json()
    #print("Write")
    #print(data)
    myresult = data.pop('result',None)
    if myresult:
    	sysid = myresult.pop('sys_id',None)
    	thereturn = sysid
    else:
    	thereturn = None 
    return thereturn

    
def get_sysid(uuid):
    url = 'https://<SN INSTANCE NAME>.service-now.com/api/now/table/cmdb_ci_vm_instance?sysparm_query=object_id%3D' + uuid + '&sysparm_limit=1'
    user = 'USERNAME'
    pwd = 'PASSWORD'
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    response = requests.get(url, auth=(user, pwd), headers=headers)
    data = response.json()
    return data 

def process_sn_db(uuid,i_state,ip,modelnumber,name,tags):
    rec = get_sysid(uuid)
    v = 0
    for a in rec['result']:
        v = 1
        if a['sys_id'] != None:
            sysid = str(a['sys_id'])
            update_sn(uuid,i_state,ip,modelnumber,name,tags,sysid)
                    

    if v == 0:
        sysid = write_sn(uuid,i_state,ip,modelnumber,name,tags)
    return sysid



###########Get Dta, Compare Current Data abd Fetch the SysID 
myrecQuery = {}
diffInstances = []
newrecs = []
myDB = db_to_use("MyNewTest", client)
myCol = Collection_to_use("TestNewCol", myDB)
start = time.time()

#####Connect to AWS and get instances
awsclient = boto3.client('ec2')
response = awsclient.describe_instances()
end = time.time()
print("It took: " + str(end - start) + " Sec to get Instances")

#######Go Over Returned Instances
start = time.time()
inscount = 0
for x in response['Reservations']:
	ins = x['Instances']
	inscount = inscount + len(x['Instances'])
	for y in x['Instances']:
		myrecQuery['id'] = y['InstanceId']
		myrecUpdate = y
		#Get current Instance if written to Mongo
		currentJSON = Query_Collection_NoLM(myrecQuery, myCol)
		aa = currentJSON
		bb = y
		aa = json.dumps(aa, default = myconverter)
		aa = json.loads(aa)
		bb = json.dumps(bb, default = myconverter)
		bb = json.loads(bb)

		####Compare the data we got from AWS and the data in Mongo Currenly and update if needed. 
		if aa != bb:
			if sys_id:
				myrecUpdate['sys_id'] = sys_id + ''
			#print("There was a miss Match")
			#####This would be rthe array of stuff that needs written to SN. Have to do this BC well SN sucks at data ingestion
			diffInstances.append(y['InstanceId'])
			#newrecs.append(y)
			rec = Update_Record(myrecQuery,bb,myCol)
		if aa == bb:
			if sys_id:
				bb['sys_id'] = sys_id + ''
			if datemod:
				bb['last_modified'] = datemod		
			rec = Update_Record_addsysID(myrecQuery,bb,myCol)

		
end = time.time()

print("Time For Mongo Process: " + str(inscount) + " Recs in :" + str(end - start) + "  Sec")			


#####Loop over all the instances that changed and push them to the SN DB
start = time.time()
for x in diffInstances:
	jquery = {}
	name = ''
	mystate = ''
	jquery['id'] = x
	docs = Query_Collection(jquery, myCol)
	for doc in docs:
		tags = []
		if all(key in doc for key in ["Tags"]):
			for tag in doc['Tags']:
				tags.append(tag['Key'] + ' : ' + tag['Value'])
				if tag['Key'] == 'Name':
					name = tag['Value']
		if doc['State']['Name'] == "running":
			mystate = "On"
		if doc['State']['Name'] == "pending":
			mystate = "Starting"
		if doc['State']['Name'] == "stopping":
			mystate = "Stopping"
		if doc['State']['Name'] == "stopped":
			mystate = "Off"
		if doc['State']['Name'] == "terminated":
			mystate = "Terminated"
		if doc['State']['Name'] == "shutting Down":
			mystate = "Stopping"

		mysys_ID = process_sn_db(doc['InstanceId'],mystate,doc['PrivateIpAddress'],doc['InstanceType'],name,str(tags))

		if mysys_ID:
			doc['sys_id'] = mysys_ID
			Update_Record(jquery,doc,myCol)
		else:
			mycount = 0
			while mycount <= 5:
				mysys_ID = process_sn_db(doc['InstanceId'],mystate,doc['PrivateIpAddress'],doc['InstanceType'],name,str(tags))
				if mysys_ID:
					doc['sys_id'] = mysys_ID
					#Update_Record(jquery,doc,myCol)
					mycount = 100
				else:
					mycount = mycount + 1
		


client.close()

end = time.time()

print("Time For SN Process: " + str(len(diffInstances)) + " Recs in " + str(end - start) + " Sec")
