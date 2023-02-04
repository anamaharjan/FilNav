
exports = function(changeEvent) {
    
    const collection_name= changeEvent.ns.coll;
    const database_name= changeEvent.ns.db;
   
    // Access the latest version of the changed document
    var docId = changeEvent.documentKey._id;
    var fullDocument = changeEvent.fullDocument;
    var time_updated = changeEvent.clusterTime;
    delete fullDocument._id;
    delete fullDocument.revision;
    delete fullDocument.time_updated;
    delete fullDocument.time_created;

    var fullDocumentBeforeChange = changeEvent.fullDocumentBeforeChange;
    if (fullDocumentBeforeChange) {

        time_created=fullDocumentBeforeChange.time_created;
        if (time_created){
            _revision=fullDocumentBeforeChange.revision;
            delete fullDocumentBeforeChange.revision;
            console.log("fullDocumentBeforeChange no r?: "+JSON.stringify(fullDocumentBeforeChange))


            _time_created=fullDocumentBeforeChange.time_created;
            if(_time_created){delete fullDocumentBeforeChange.time_created;}
    
            _time_updated=fullDocumentBeforeChange.time_updated;
            if(_time_updated){delete fullDocumentBeforeChange.time_updated;}
            delete fullDocumentBeforeChange._id;
            console.log("fullDocumentBeforeChange: "+JSON.stringify(fullDocumentBeforeChange))
            console.log("fullDocument: "+JSON.stringify(fullDocument))
            if(JSON.stringify(fullDocument)!=JSON.stringify(fullDocumentBeforeChange)){
                //console.log("change detected in new fullDocument!")
                fullDocumentBeforeChange.__id=docId;
                const updateDescription = changeEvent.updateDescription;
                if (updateDescription) {
                    fullDocumentBeforeChange._update=updateDescription;
                }
                fullDocumentBeforeChange._update.operation_type=changeEvent.operationType

                //console.log('going to findOneAndUpdate _id: '+docId+' db: '+database_name+' collection: '+collection_name);
                context.services.get("entity-id").db(database_name).collection(collection_name).findOneAndUpdate({"_id":docId},{$set:{"revision":_revision+1,"time_updated":time_updated,"time_created":time_created}});

                fullDocumentBeforeChange.time_created=_time_created;
                fullDocumentBeforeChange.time_updated=_time_updated;
                if(_revision){fullDocumentBeforeChange.revision=_revision;}

                //console.log('going to insertOne into revision_db collection '+collection_name)
                //console.log(JSON.stringify(fullDocumentBeforeChange))
                context.services.get("entity-id").db("revision_db").collection(collection_name).insertOne(fullDocumentBeforeChange);
            }else{
                //console.log("no change detected for doc")
            }    

        }
        else{
            //console.log('previous doc, but no time_created')
            context.services.get("entity-id").db(database_name).collection(collection_name).findOneAndUpdate({"_id":docId},{$set:{"revision":0,"time_created":time_updated,"time_updated":time_updated}});  
        }
    }else{
        //console.log("no fullDocumentBeforeChange")
        context.services.get("entity-id").db(database_name).collection(collection_name).findOneAndUpdate({"_id":docId},{$set:{"revision":0,"time_created":time_updated,"time_updated":time_updated}});
    }
  };
