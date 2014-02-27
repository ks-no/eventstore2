package no.ks.eventstore2.store;


import com.mongodb.BasicDBObject;
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
        //TODO: index on jid and data also? Index support covered query
        BasicDBObject indexes = new BasicDBObject("projectionId", 1).append("dataVersion", 1).append("hostname", 1);
        db.getCollection("snapshot").ensureIndex(indexes);
    }

    public void open() {
        if (db == null) {
            db = mongoClient.getDB("SnapshotRepository");
        }
    }

    public DBCollection getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            db = null;
        }
    }

    public DB getDb() {
        return db;
    }
}
