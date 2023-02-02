
  exports = async function(changeEvent) {
    const operation_type=changeEvent.operationType
    const algoliasearch = require('algoliasearch');
    const client = algoliasearch("$ALGOLIA_ID","$ALGOLIA_KEY");
    
    const index = client.initIndex("miners");
    
    if (operation_type=="delete"){
        try{
        const result = await index.deleteObject(changeEvent.documentKey._id);
        console.log(Date.now(),'successfully deleted: ',result);
        }
        catch(e){
        console.error(e);
        }
    }
    else{
        var algolia_object = new Object();
        algolia_object.objectID = changeEvent.documentKey._id;
        
        full_document = changeEvent.fullDocument;
        algolia_object.miner_id=full_document.miner_id;

        if(full_document.names){algolia_object.names = full_document.names;}


        try{
        const result = await index.saveObject(algolia_object);
        console.log(Date.now(),'successfully updated: ',JSON.stringify(result));
        }
        catch(e){
        console.error(e);
        }
    }
  }
