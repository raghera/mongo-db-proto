package com.ravi.mongo;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.ravi.mongo.dao.MongoDao;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static java.util.Arrays.asList;

public class MongoConnectorApp {

    private static final String COLL_NAME = "restaurants";



    public static void main(String [] args) throws Exception {
        MongoDao dao = new MongoDao();

        System.out.println(" >>>> " + dao.getCollection(COLL_NAME).count());

        dao.insertOneDocument(COLL_NAME, buildDocument());

        System.out.println(" >>>> " + dao.getCollection(COLL_NAME).count());


//        final FindIterable<Document> iterable = dao.findAllDocuments(COLL_NAME);
//        printCollection(iterable);

//        final FindIterable<Document> filteredResult = dao.findDocuments(COLL_NAME, buildFilterDoc("borough", "Manhattan"));
//        printCollection(filteredResult);

        final FindIterable<Document> embeddedResult = dao.findDocuments(COLL_NAME, buildFilterDoc());
        printCollection(embeddedResult);

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
                .append("grades", asList(
                        new Document()
                                .append("date", format.parse("2014-10-01T00:00:00Z"))
                                .append("grade", "A")
                                .append("score", "11"),
                        new Document()
                                .append("date", format.parse("2014-01-16T00:00:00Z"))
                                .append("grade", "B")
                                .append("score", "17")))
                .append("name", "Vella")
                .append("restaurant_id", "41704620"));

        return document;
    }



}
