package no.ks.eventstore2.store;


import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

public class MongoDbStore {

    public static final Logger log = LoggerFactory.getLogger(MongoDbStore.class);

    private MongoClient mongoClient;
    private DB db;

    public MongoDbStore(MongoClient client, String name) throws UnknownHostException {
        this.mongoClient = client;
        db = mongoClient.getDB(name);
    }

    public DBCollection getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    public DB getDb() {
        return db;
    }
}
