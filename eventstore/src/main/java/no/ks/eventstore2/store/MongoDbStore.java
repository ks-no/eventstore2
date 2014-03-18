package no.ks.eventstore2.store;


import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoDbStore {

    private DB db;

    public MongoDbStore(MongoClient mongoClient, String name) {
        db = mongoClient.getDB(name);
    }

    public DBCollection getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    public DB getDb() {
        return db;
    }
}
