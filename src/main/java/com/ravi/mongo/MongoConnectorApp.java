package com.ravi.mongo;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static java.util.Arrays.asList;

public class MongoConnectorApp {

    private static final String COLLECTION_NAME = "restaurants";


    public static void main(String[] args) throws Exception {
        useMongoWithMongoJavaClient();
    }

    private static void useMongoWithMongoJavaClient() throws Exception {
        final com.ravi.mongo.dao.MongoDao dao = new com.ravi.mongo.dao.MongoDao();

        System.out.println(" >>>> " + dao.getCollection(COLLECTION_NAME).count());

        dao.insertOneDocument(COLLECTION_NAME, buildDocument());

        System.out.println(" >>>> " + dao.getCollection(COLLECTION_NAME).count());

//        final FindIterable<Document> iterable = dao.findAllDocuments(COLLECTION_NAME);
//        printCollection(iterable);
//
//        final FindIterable<Document> filteredResult = dao.findDocuments(COLLECTION_NAME, buildFilterDoc("borough", "Manhattan"));
//        printCollection(filteredResult);

        //find using Filters api (simpler)
//        final FindIterable<Document> result = dao.findDocumentsUsingFilter(COLLECTION_NAME, "borough", "Manhattan");
//        printCollection(result);

        //Iterate to check results.
//        result.forEach(new java.util.function.Consumer<Document>() {
//            @Override
//            public void accept(Document document) {
//                final Object obj = document.get("borough");
//                if (obj instanceof String && (!((String) obj).equals("Manhattan"))) {
//                    throw new RuntimeException("A value was not correct from the DB");
//                }
//            }
//        });

//        final FindIterable<Document> embeddedResult = dao.findDocuments(COLLECTION_NAME, buildFilterDoc());
//        printCollection(embeddedResult);

        //find greater than
        FindIterable<Document> iterable = dao.findGreaterThan(COLLECTION_NAME, "grades.score",30);
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
//                if(document.containsKey()))
                System.out.println(document);
            }
        });

        //find using multiple params
//        java.util.Map.Entry<String, String> cuisine = new java.util.AbstractMap.SimpleEntry<String, String>("cuisine", "Italian");
//        java.util.Map.Entry<String, String> zipcode = new java.util.AbstractMap.SimpleEntry<String, String>("address.zipcode", "10075");
//
//        FindIterable<Document> iterable = dao.findCuisineInArea(COLLECTION_NAME, cuisine, zipcode );
//        printCollection(iterable);

//        iterable.sort(new Document("borough", 1).append("address.zipcode", 1) );//ascending
        iterable.sort(new Document("borough", -1).append("address.zipcode", -1) );//descending
        System.out.println(" ============ SORTED ==========");
        printCollection(iterable);

    }

    private static void printCollection(FindIterable<Document> iterable) {
        iterable.forEach(new Block<Document>() {
            public void apply(Document document) {
                System.out.println(document);
            }
        });
    }

    //Filter by a top level element
    private static Document buildFilterDoc(String key, String value) {
        return new Document(key, value);
    }

    //Filter by an embedded element (use dot notation)
    private static Document buildFilterDoc() {
        return new Document("address.building", "1480");
    }

    private static Document buildDocument() throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

        Document document = new Document("address",
                new Document()
                        .append("street", "2 Avenue")
                        .append("zipcode", "10075")
                        .append("building", "1480")
                        .append("coord", asList(-73.9557413, 40.7720266))
                        .append("borough", "Manhattan")
                        .append("cuisine", "Italian")
                        .append("grades", asList(new Document()
                                .append("date", format.parse("2014-10-01T00:00:00Z"))
                                .append("grade", "A")
                                .append("score", "11"), new Document()
                                .append("date", format.parse("2014-01-16T00:00:00Z"))
                                .append("grade", "B")
                                .append("score", "17")))
                .append("name", "Vella")
                .append("restaurant_id", "41704620"));

        return document;
    }

}
