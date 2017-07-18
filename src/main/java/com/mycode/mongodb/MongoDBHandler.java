package com.mycode.mongodb;

import com.mongodb.MongoClient;

public class MongoDBHandler {

    private MongoClient mongo = null;

    private MongoDBHandler(){
        mongo = new MongoClient("MONGODB_SERVER_HOST",
                Integer.parseInt("MONGODB_SERVER_PORT"));
    }

    public MongoClient getMongoClient(){
        return mongo;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Static part
    private static MongoDBHandler mongoHandler = null;

    public static synchronized MongoDBHandler getInstance(){

        if (mongoHandler == null){
            mongoHandler = new MongoDBHandler();
        }

        return mongoHandler;
    }

    public static void init(){

        if (mongoHandler == null){
            mongoHandler = new MongoDBHandler();
        }
    }
}
