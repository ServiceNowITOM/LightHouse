import datetime
import pymongo
import sys
from flask import Flask, jsonify
from flask import request
from flask import make_response
import json
from bson import json_util
app = Flask(__name__)

##Create a MongoDB client and open connection to Amazon DocumentDB
client = pymongo.MongoClient('MONGODB CONNECTION-STRING')
def myconverter(o):
	if isinstance(o, datetime.datetime):
		o.replace(tzinfo=None)
		millisec = o.timestamp() * 1000
		return (o.timestamp() * 1000) - 1000 


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
        mydate = datetime.datetime.utcnow()
        Json_Update['last_modified'] = mydate.strftime('%Y-%m-%d %H:%M:%S+00:00')
        myObj = {}
        myObj = {"$set": Json_Update}
        results = collection.update_one(Json_Query,myObj,True)
        return results

##Query and print Record
def Query_Collection(Json_Query, collection):
        docs = collection.find(Json_Query)
        return docs


#Get and return Doc By SysID
def sysIDquery_Collection(sysid, collection):
		myquery = {}
		myquery.sys_id = sysid
		docs = collection.find(myquery)
		return docs
        
#Query for attribute and return matching Sys_ID
def matchingquery_Collection(DocQuery, Collection):
		sys_ids = []
		docs = collection.find(DocQuery)
		for doc in docs:
			sys_ids.append(doc.sys_id)
		return sys_ids
		


##Error
Error = {
        'Error': 500,
        'Error Message': u'Bad request malformed JSON. System Expects {"DB": "Name of Database", "Collection": "Name of Collection", "Payload": "<Your JSON Payload>", "Query": "<Your JSON Query>"}',
        'done': False
        }
##End Error 
##Success Message
Success = {
        'ExitCode': 201,
        'Message': u'Payload was written',
        'Document': u'',
        'done': True
        }
##End Success Message

##Error Writing to DB
Error = {
        'Error': 550,
        'Error Message': u'Error writing to DB',
        'Error Details': u'',
        'done': False
        }
##End Writing Error

@app.route('/lighthouse/api/v1.0/Update', methods=['POST'])
def get_tasks():
#       if not request.json or not "DB" in request.json or not "Collection" in request.json:
#               return jsonify(Error), 500

        requestPayload = request.json['payload']
        requestCol = request.json['Collection']
        requestDB = request.json['DB']
        requestQuery = request.json['Query']
        myDB = db_to_use(requestDB, client)
        myCol = Collection_to_use(requestCol, myDB)
        rec = Update_Record(requestQuery,requestPayload,myCol)
        myDocs = Query_Collection(requestQuery, myCol)
        for doc in myDocs:
#               Success['Document'] = doc
                print(doc)
        return jsonify(Success), 201
#       return jsonify(request.json), 201

##Write Updates to the system


@app.route('/lighthouse/api/v1.0/Query/raw', methods=['POST'])
def get_rawdoc():
	print(request.json)
	requestCol = request.json['Collection']
	requestDB = request.json['DB']
	requestQuery = request.json['Query']
	myDB = db_to_use(requestDB, client)
	myCol = Collection_to_use(requestCol, myDB)
	#rec = Update_Record(requestQuery,requestPayload,myCol)
	myobj = {}
	myobj['Results'] = []
	myDocs = Query_Collection(requestQuery, myCol)
	for doc in myDocs:
		doc.pop('_id', None)
		myobj['Results'].append(doc)
		#print(doc)
	return json.dumps(myobj, default = myconverter), 201
#       return jsonify(request.json), 201



@app.route('/lighthouse/api/v1.0/Query/SYSID', methods=['POST'])
def get_sysids():
#       if not request.json or not "DB" in request.json or not "Collection" in request.json:
#               return jsonify(Error), 500

        requestCol = request.json['Collection']
        requestDB = request.json['DB']
        requestQuery = request.json['Query']
        myDB = db_to_use(requestDB, client)
        myCol = Collection_to_use(requestCol, myDB)
        #rec = Update_Record(requestQuery,requestPayload,myCol)
        myDocs = Query_Collection(requestQuery, myCol)
        sysIDs = []
        for doc in myDocs:
        	sysIDs.append(doc['sys_id'])
        	#Success['Document'] = doc
        	#print(doc)
        return json.dumps(sysIDs), 201
#       return jsonify(request.json), 201


if __name__ == '__main__':
        app.run(debug=True,host="0.0.0.0",port=8080)
