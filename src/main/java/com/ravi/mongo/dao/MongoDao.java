package com.ravi.mongo.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.gt;

/**
 * Assumes a local mongodb is running on 127.0.0.1 with a DB called test
 * and primer data loaded from:
 * /Users/raghera/mongodb/data/gettingstarted/primer-dataset.json
 */
public class MongoDao {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 27017;
    private static final String DATABASE_NAME = "test";

    private MongoDatabase db;

    //TODO propertly lazy load this as a singleton
    public MongoDao() {
        MongoClient mongoClient = new MongoClient(HOST, PORT);
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

    public FindIterable<Document> findDocumentsUsingFilter(final String collectionName, String name, String value) {
        return db.getCollection(collectionName).find(com.mongodb.client.model.Filters.eq(name, value));
    }

    public FindIterable<Document> findGreaterThan(final String collectionName, String key, int value) {
//        return db.getCollection("restaurants").find(
//                new Document(key, new Document("$gt", value)));
        //or
        return db.getCollection(collectionName).find(gt(key, 30));

    }

    public FindIterable<org.bson.Document> findCuisineInArea(final String collectionName,
                                                             java.util.Map.Entry<String, String> cuisine,
                                                             java.util.Map.Entry<String, String> zipcode) {

        return db.getCollection(collectionName).find(
                new Document(cuisine.getKey(), cuisine.getValue())
                        .append(zipcode.getKey(), zipcode.getValue()));

    }




}
