const express = require('express');
const db = require("./db");
var app = express();
var bodyParser = require("body-parser");

// parses json data sent to us by the user 
app.use(bodyParser.json());
// Connecting to database
db.connect((err)=>{
    // If err unable to connect to database
    // End application
    if(err){
        console.log('Unable to connect to database');
        process.exit(1);
    }else{
         app.listen(3000,()=>{
            console.log('Successfully Connected to database, app listening on port 3000');
            console.log(`\n 1. When you try to connect to localhost:3000 csv data will be loaded to db.
            	\n 2. You can use POST/getPolicyInfo(input JSON body) to get policy info by using username Ex: { "name" : "Lura Lucca"}.
				\n 3. You can use GET/getAggregate to get aggregate policy of each user.`)
        });
    }
});

app.get('/',(req,res) =>{
	db.uploadDataToDb((err,result) =>{
		if(err)
			res.json("Error while loading data to database please try to reconnect again.!");
		else
			res.json(result);
	})
})

app.post('/getPolicyInfo',(req,res) => {
	var userName = req.body.name;
	// console.log("username ",userName)
	db.getPolicyInfo(userName,function(err,param){
    	if(err)
    		console.log("Error while fetching data please try again");
    	else if(param && param == "NF")
    		res.json("User not found please try providing a correct name");
    	else
    		res.json(param);
	})
})


app.get('/getAggregate',(req,res) => {
	db.showAggregation(function(err,params){
    	if(err)
    		res.json("Error while fetching data please try again");
    	else
    		res.json(params);
	})
})
