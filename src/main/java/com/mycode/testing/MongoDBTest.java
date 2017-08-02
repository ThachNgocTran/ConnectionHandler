package com.mycode.testing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mycode.mongodb.MongoDBHandler;
import org.apache.http.util.Args;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MongoDBTest {

    private static final String CLASS_NAME = MongoDBTest.class.getSimpleName();
    private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

    /*
    *** Context ***
    Supposed in MongoDB, we have one database named "databaseName", containing one collection named "collectionName".
    Each document within this collection contains a field named "_id", which is automatically created when uploading data to MongoDB.
    This id uniquely identifies that document.
    In this case, assumed each document only contains flat key/value pairs.
    *** Func call ***
    testGetOneDocumentBasedOnId("595b8f013d695127bcf9207b")
    Return the document having the specified id.
     */
    public static Map<String, String> testGetOneDocumentBasedOnId(String id){

        MongoCursor<Document> curs = null;
        Map<String, String> res = Maps.newHashMap();

        try{

            Args.notEmpty(id, "id");

            MongoClient mc = MongoDBHandler.getInstance().getMongoClient();
            MongoDatabase db = mc.getDatabase("databaseName");
            MongoCollection collection = db.getCollection("collectionName");

            BasicDBObject whereQuery = new BasicDBObject();
            whereQuery.put("_id", new ObjectId(id));

            curs = collection.find(whereQuery).iterator();

            if (!curs.hasNext()){
                throw new RuntimeException(String.format("Unable to find object with id [%s].", id));
            }

            Document obj = curs.next();

            for(String key: obj.keySet()){
                res.put(key, obj.get(key).toString());
            }
        }
        finally {
            if (curs != null)
                curs.close();   // Must close the cursor in order to avoid resource waste in MongoDB.
        }

        return res;
    }

    /*
    *** Context ***
    Supposed in MongoDB, we have one database named "databaseName", containing one collection named "collectionName".
    Each document within this collection may contain a varied set of headers. But all of them share, let's say, field 1 to field 5.
    And we want to extract documents that have field1 EQUALLING conditionForField1, field2 EQUALLING conditionForField2.
    *** Func call ***
    testGetDocumnetsBasedOnConditions("someText1", "someText2")
    Return the documents that satisfy the two conditions. (exact match!)
     */
    public static List<Map<String, String>> testGetDocumnetsBasedOnConditions(String conditionForField1,
                                                                              String conditionForField2){

        MongoCursor<Document> curs = null;
        List<Map<String, String>> res = Lists.newArrayList();

        try{

            MongoClient mc = MongoDBHandler.getInstance().getMongoClient();
            MongoDatabase db = mc.getDatabase("databaseName");
            MongoCollection collection = db.getCollection("collectionName");

            curs = collection.find(and(eq("field1", conditionForField1),
                    eq("field2", conditionForField2))).iterator();

            while(curs.hasNext()){

                Document obj = curs.next();
                HashMap<String, String> tempHm = Maps.newHashMap();

                for(String key: obj.keySet()){
                    tempHm.put(key, obj.get(key).toString());
                }

                res.add(tempHm);
            }
        }
        finally {
            if (curs != null)
                curs.close();   // Must close the cursor in order to avoid resource waste in MongoDB.
        }

        return res;
    }

    /*
    We can write a long javascript code in a file (*.js), load it into "alllines", and execute them
    inside MongoDB Server, exactly the same as we open the file manually, copy/paste the code into
    MongoDB Console!
     */
    public static void testExecuteJavascriptCode(List<String> alllines){

        String strMerging = String.join("\n", alllines);
        BasicDBObject commMerging = new BasicDBObject();
        commMerging.put("eval", String.format("function(){%s}", strMerging));

        MongoDatabase db = MongoDBHandler.getInstance().getMongoClient().getDatabase("databaseName");
        Document resultMerging = db.runCommand(commMerging);

        LOGGER.info("Result from function call:");

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(resultMerging.toJson());
        String prettyJsonString = gson.toJson(je);

        LOGGER.info(prettyJsonString);
    }
}
