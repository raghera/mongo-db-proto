package com.ravi.mongo.dao;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Map.Entry;

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
                                                             Entry<String, String> cuisine,
                                                             Entry<String, String> zipcode) {

        return db.getCollection(collectionName).find(
                new Document(cuisine.getKey(), cuisine.getValue())
                        .append(zipcode.getKey(), zipcode.getValue()));

    }

    //Updates the FIRST record ONLY - found by the find key/value with a new key/value pair - these can be embedded attributes or not.
    //Also updates the lastModified field with the currentDate using a driver operator.
    public UpdateResult updateOneRecord(final String collectionName, Entry<String, String> findKV, Entry<String, String> newKV) {
        return db.getCollection(collectionName).updateOne(new Document(findKV.getKey(), findKV.getValue()),
                new Document("$set", new Document(newKV.getKey(), newKV.getValue()))
                        .append("$currentDate", new Document("lastModified", true)));

    }

    //Updates all records where it matches the zipcode & cuisine = other
    public UpdateResult updateManyRecords(final String collectionName,
                                          Entry<String, String> find1KV,
                                          Entry<String, String> find2KV,
                                          Entry<String, String> newKV) {
        return db.getCollection(collectionName).updateMany(new Document(find1KV.getKey(), find1KV.getValue())
                        .append(find2KV.getKey(), find2KV.getValue()),
                new Document("$set", new Document(newKV.getKey(), newKV.getValue()))
                        .append("$currentDate", new Document("lastModified", true)));
    }

    //Replaces the entire document
    public UpdateResult replaceOne(final String collectionName, String documentId, Document replacement) {
//        return db.getCollection(collectionName).replaceOne(new Document("restaurant_id", "41704620"), replacement);
        return db.getCollection(collectionName).replaceOne(new Document("restaurant_id", documentId),
                replacement,
                new UpdateOptions().upsert(true)); //upsert means if nothing is matched then we insert a new row instead.
    }

    //Remove all that meet the criteria - you can also deleteOne which removes the first only.
    public DeleteResult remove(final String collectionName, Entry<String, String> findKV) {
        return db.getCollection(collectionName).deleteMany(new Document(findKV.getKey(), findKV.getValue()));
    }


}
