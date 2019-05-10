const db = require("./db");

// Connecting to database
db.connect((err)=>{
    // If err unable to connect to database
    // End application
    if(err){
        console.log('Unable to connect to database');
        process.exit(1);
    }else{
        console.log('Successfully Connected to database');
    }
});

var timer = setInterval(() => {
	if(db.isDataImported()){
		console.log("Data Has Been Loaded To MongoDB..!")
		clearInterval(timer);
		proceedToNextTask();
	}
})

var proceedToNextTask = () =>{
	var stdin = process.openStdin();
	console.log(`\n\nType the username to find Policy info.`)
	console.log(`Press 1 to see the aggregated policy by each user.\n`)
	stdin.addListener("data", function(arg) {
	    var input = arg.toString().trim();
	    console.log("you entered: "+ input);
	    console.log("Searhing the info.....");
	    if(input == 1){
	    	console.log("Fetching aggregated plicies...")
	    	db.showAggregation(function(err,res){
		    	if(err)
		    		console.log("Error while fetching data please try again");
		    	else
		    		console.log(res)
	    	})
	    }else{
	    	db.getPolicyInfo(input,function(err,res){
		    	if(err)
		    		console.log("Error while fetching data please try again");
		    	else if(res && res == "NF")
		    		console.log("User not found please try providing a correct name");
		    	else
		    		console.log(res)
	    	})
	    }
	});
}