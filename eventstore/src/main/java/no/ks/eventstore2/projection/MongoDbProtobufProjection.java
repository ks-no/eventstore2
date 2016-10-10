package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bson.Document;

import java.io.ByteArrayInputStream;

public abstract class MongoDbProtobufProjection extends ProjectionProtobufSnapshot {

    private final MongoDatabase mongodatabase;
    private GridFSBucket gridFS;

    private static String nodename = System.getProperty("nodename") != null ? System.getProperty("nodename") : "local";

    public MongoDbProtobufProjection(ActorRef eventStore, MongoClient client) {
        super(eventStore);
        mongodatabase = client.getDatabase(nodename + "_SnapshotRepository");
        gridFS = GridFSBuckets.create(mongodatabase, nodename + "_snapshot_data");
        mongodatabase.getCollection("snapshot").createIndex(new BasicDBObject("dataVersion", 1).append("projectionId", 1));
    }

    @Override
    public void saveSnapshot() {
        final String simpleName = getClass().getSimpleName();
        log.info("{} Saving snapshot for event {}", simpleName, latestJournalidReceived);

        final byte[] data = serializeData();
        log.info("{} serialized data, storing to db", simpleName);
        context().dispatcher().execute(() -> {
            try {
                MongoCollection<Document> collection = mongodatabase.getCollection("snapshot");
                if (latestJournalidReceived != 0) {
                    collection.updateOne(new Document("_id", getId()), new Document("$set", new Document("jid", latestJournalidReceived).append("dataVersion", getSnapshotDataVersion()).append("projectionId", simpleName)), new UpdateOptions().upsert(true));

                    saveDataToGridFS(data);

                    log.info("{} Saved snapshot for event {}", simpleName, latestJournalidReceived);
                }
            } catch (Exception e) {
                log.error("Failed to write snapshot", e);
            }
        });
    }


    @Override
    public void loadSnapshot() {
        log.info("{} loading snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);


        try {
            MongoCollection<Document> collection = mongodatabase.getCollection("snapshot");
            Document query = new Document("projectionId", getClass().getSimpleName())
                    .append("dataVersion", getSnapshotDataVersion());


            Document document = collection.find(query).limit(1).first();

            if (document != null)  {
                Long latestJournalIdSnapshoted = document.getLong("jid");

                Document fileQuery = new Document("filename", getId());
                GridFSFile file = gridFS.find(fileQuery).limit(1).first();
                if (file != null) {
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    gridFS.downloadToStream(file.getObjectId(), outputStream);
                    deSerializeData(outputStream.toByteArray());
                }

                if (latestJournalIdSnapshoted != 0) {
                    latestJournalidReceived = latestJournalIdSnapshoted;
                    log.info("loaded snapshot for event {}", latestJournalidReceived);
                }
            }
        } catch (Exception e ) {
            log.error("Failed to load snapshot for {}", getClass().getSimpleName(), e);
            latestJournalidReceived = 0;
        } finally {

        }
    }

    protected void saveDataToGridFS(byte[] data) {
        gridFS.uploadFromStream(getId(), new ByteArrayInputStream(data));
    }

    private String getId() {
        return "v_" + getSnapshotDataVersion() + "_" + this.getClass().getName();
    }

    protected abstract byte[] serializeData();

    protected abstract void deSerializeData(byte[] bytes);

    /**
     * Data version this projection uses
     *
     * @return
     */
    protected abstract String getSnapshotDataVersion();


}
