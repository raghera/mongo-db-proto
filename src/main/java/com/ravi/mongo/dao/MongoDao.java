package com.ravi.mongo.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * Assumes a local mongodb is running on 127.0.0.1 with a DB called test
 * and primer data loaded from:
 * /Users/raghera/mongodb/data/gettingstarted/primer-dataset.json
 */
public class MongoDao {

    private static final String DATABASE_NAME = "test";

    private MongoDatabase db;

    //TODO propertly lazy load this as a singleton
    public MongoDao() {
        MongoClient mongoClient = new MongoClient();
        db = mongoClient.getDatabase(DATABASE_NAME);
    }

    public MongoCollection<Document> getCollection(final String collectionName) {
        return db.getCollection(collectionName);
    }

    public void insertOneDocument(final String collectionName, final Document document) {
        getCollection(collectionName).insertOne(document);
    }

    public FindIterable<Document> findAllDocuments(final String collectionName) {
        return db.getCollection(collectionName).find();
    }

    public FindIterable<Document> findDocuments(final String collectionName, final Document filterDoc) {
        return db.getCollection(collectionName).find(filterDoc);
    }



}
