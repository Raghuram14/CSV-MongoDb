const fs = require('fs');
const csv = require('csv-parser');
const async = require('async');

const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const mongoOptions = {useNewUrlParser : true};
const url = "mongodb://localhost:27017";
const dbName = 'csv';
const csvFile = 'Sample Sheet.csv';
var db = null;
var csvData = [];
var sortedData = {};

const connect = (cb) =>{
    if(db)
        cb();
    else{
        // attempt to get database connection
        MongoClient.connect(url,mongoOptions,(err,client)=>{
            if(err)
                cb(err);
            else{
				db = client.db(dbName);
            	cb();
            }
        });
    }
}


var uploadDataToDb = (cb) =>{
	db.collection('User').estimatedDocumentCount(function(err,res){
		if(err){
			cb(err);
		}else if(res >10){ // to check whether data has already been loaded or not
			cb(null,"Data have been loaded successfully to the database...!");
		}else{
			readData(function(){
				createUniqueIndex();
				sortData(()=>{
					pushToInsertData(() => {
    					cb(null,"Data have been loaded successfully to the database...!");
					})
				});
			})
		}
	})
}

var readData = (cb) =>{
	fs.createReadStream(csvFile)
	  .pipe(csv())
	  .on('data', function (row) {
	    csvData.push(row);
	  })
	  .on('end', function () {
	    console.log('csvData loaded \n Loading data to database');
	    cb();
	  })
}

var sortData = (callback) =>{
	async.eachSeries(Object.keys(csvData), function (index, next){ 
	// for(let index in csvData){
		var userInfo = [];
		var policyInfo = [];
		var agentInfo = csvData[index]['agent'];
		var accountName = csvData[index]['account_name'];
		var lobInfo = csvData[index]['category_name'];
		var policayCarrier = csvData[index]['company_name'];

		policyInfo.push(csvData[index]['policy_number']);
		policyInfo.push(csvData[index]['policy_start_date']);
		policyInfo.push(csvData[index]['policy_end_date']);
		
		userInfo.push(csvData[index]['firstname']);
		userInfo.push(csvData[index]['gender']);
		userInfo.push(csvData[index]['dob']);
		userInfo.push(csvData[index]['address']);
		userInfo.push(csvData[index]['phone']);
		userInfo.push(csvData[index]['city']);
		userInfo.push(csvData[index]['state']);
		userInfo.push(csvData[index]['zip']);
		userInfo.push(csvData[index]['email']);
		userInfo.push(csvData[index]['userType']);

		sortedData[index] = {};
		sortedData[index]['agent'] = agentInfo;
		sortedData[index].accountName = accountName;
		sortedData[index].lob = lobInfo;
		sortedData[index].carrier = policayCarrier;
		sortedData[index].policy = policyInfo;
		sortedData[index].user = userInfo;
		next();
	})
	callback();
}

var commonInsertFunc = (collection,insertData,Type,index) =>{
	collection.insertOne(insertData,(err,result) =>{
		if(err){
			// console.log(`Error in insertPolicayData : ${err}`);
			// process.exit(1)
			return;
		}
		// console.log(`No of records ${Type} (result.result.n): ${result.result.n}`)
        // console.log(`No of records ${Type} (result.ops.length): ${result.ops.length}`)
	})
}


var createUniqueIndex = () =>{
	db.collection('Agent').createIndex({Agent_Name:1}, {unique:true}, (err, result) => {
    	if(err) {console.error(`Failed to create index ${err}`); process.exit(1);}
    	// console.log(`Unique Index created successfully: ${result}`)
	})
	db.collection('Policy Carrier').createIndex({Company_Name:1}, {unique:true}, (err, result) => {
    	if(err) {console.error(`Failed to create index ${err}`); process.exit(1);}
    	// console.log(`Unique Index created successfully: ${result}`)
	})
	db.collection('Policy Category(LOB)').createIndex({Category_Name:1}, {unique:true}, (err, result) => {
    	if(err) {console.error(`Failed to create index ${err}`); process.exit(1);}
    	// console.log(`Unique Index created successfully: ${result}`)
	})
}

var pushToInsertData = (callback) =>{
	async.eachSeries(Object.keys(sortedData), function (index, next){ 
		// Inserting Agent Details
		commonInsertFunc(db.collection('Agent'),{Agent_Name : sortedData[index]['agent']},"Agent");

		commonInsertFunc(db.collection('Policy Category(LOB)'),{
			Category_Name : sortedData[index]['lob']
		},'LOB')

		commonInsertFunc(db.collection('Policy Carrier'),{
			Company_Name : sortedData[index]['carrier']
		},'Policy_Carrier')

		// Inserting User Details
		var userData = {
			_id : Number(index) + 1,
			Firstname : sortedData[index]['user'][0],
			Gender : sortedData[index]['user'][1],
			DOB : sortedData[index]['user'][2],
			Address : sortedData[index]['user'][3],
			Phone_Number : sortedData[index]['user'][4],
			City : sortedData[index]['user'][5],
			State : sortedData[index]['user'][6],
			Zip_Code : sortedData[index]['user'][7],
			Email : sortedData[index]['user'][8],
			UserType : sortedData[index]['user'][9]
			// Agent_ID : db.collection('Agent').find({"Agent_Name":sortedData[index]['agent']},{"Agent_Name":0})
		}
		commonInsertFunc(db.collection('User'),userData,'User',index);

		// Inserting user-account details
		commonInsertFunc(db.collection('User\'s Account'),{
			_id : Number(index) + 1,
			Account_Name : sortedData[index]['accountName']
		},'User Account',index)

		var Company_id = null;
		var Category_id = null;
		fetchIds(db.collection('Policy Category(LOB)'),
			{Category_Name : sortedData[index]['lob']},
			function(catID){
				Category_id = catID;
				fetchIds(db.collection('Policy Carrier'),
					{Company_Name : sortedData[index]['carrier']},
					function(comID){
						Company_id = comID || null;
						commonInsertFunc(db.collection('Policy Info'),{
							Policy_Number : sortedData[index]['policy'][0],
							Policy_start_date : sortedData[index]['policy'][1],
							Policy_end_date : sortedData[index]['policy'][2],
							Category_id : Category_id,
							Company_id : Company_id,
							User_id : Number(index) + 1,
						},'Policy_Info')
						next();
					}
				);
			}
		);		
	}, function(err) {
		callback();
	}); 
}


function fetchIds(collection,searchKey,callback){
	collection.findOne(searchKey,function(error,res){
		if(error){
			console.log(`Error While fetching data for ${searchKey}`);
		}
		else{
			if(res && res._id)
				callback(ObjectID(res._id));
			else
				callback()
		}
	})
}

// returns database connection 
const getDB = ()=>{
    return db;
}

const getPolicyInfo = (userName,callback) =>{
	db.collection('User').findOne({Firstname : userName},function(error,res){
		if(error){
			console.log(`Error While fetching data for ${userName}`);
		}
		else{
			// console.log(res)
			if(res && res._id){
				db.collection('Policy Info').findOne({User_id : res._id},function(error,res){
				// console.log(error,res)
					if(error)
						callback(true);
					else
						callback(null,res);
				})
			}
			else{
				callback(null,"NF");	
			}
		}
	})
}

async function showAggregation(callback){
	var docs = await db.collection('Policy Info').aggregate([
		{$skip : 0},{$limit : 100},
		{$lookup : { from : "User",
			localField : "User_id",
			foreignField : "_id",
			as : "UserInfo"
		}},
		{$lookup : { from : "Policy Category(LOB)",
			localField : "Category_id",
			foreignField : "_id",
			as : "LOB"
		}},
		{$lookup : { from : "Policy Carrier",
			localField : "Company_id",
			foreignField : "_id",
			as : "Carrier"
		}},
		{$project: {Policy_Number:1,Policy_start_date:1,Policy_end_date:1,_id:0,
			// UserInfo: { $arrayElemAt: [ "$UserInfo", 0 ]},
            LOB: { $arrayElemAt: [ "$LOB", 0 ] },
            Carrier: { $arrayElemAt: [ "$Carrier", 0 ] }
        }}
	]).toArray();

	if(docs)
		callback(null,docs);
	else 
		callback(true);
}

process.on('unhandledRejection', error => {
  // Prints "unhandledRejection woops!"
  console.log('unhandledRejection woops!');
});
module.exports = {getDB,connect,getPolicyInfo,showAggregation,uploadDataToDb};