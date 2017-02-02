package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.UUID;

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
                    final String fileid = UUID.randomUUID().toString();
                    collection.updateOne(new Document("_id", getId()), new Document("$set", new Document("jid", latestJournalidReceived).append("dataVersion", getSnapshotDataVersion()).append("projectionId", simpleName).append("fileid", fileid)), new UpdateOptions().upsert(true));
                    saveDataToGridFS(data, fileid);
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

        if(latestJournalidReceived != 0){

            log.error("Snapshot allerede lastet laster ikke på nytt.", new RuntimeException());
            return;
        }

        try {
            MongoCollection<Document> collection = mongodatabase.getCollection("snapshot");
            Document query = new Document("projectionId", getClass().getSimpleName())
                    .append("dataVersion", getSnapshotDataVersion());


            Document document = collection.find(query).limit(1).first();

            if (document != null)  {
                Long latestJournalIdSnapshoted = document.getLong("jid");

                final String fileid = document.getString("fileid");
                if(fileid == null) {
                    log.error("Snapshot har ikke fileid");
                    return;
                }
                Document fileQuery = new Document("filename", getId() + fileid);
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

    protected void saveDataToGridFS(byte[] data, String fileid) {
        Bson fileQuery = Filters.regex("filename", getId() + ".*");
        try {
            final ArrayList<GridFSFile> es = new ArrayList<>();
            gridFS.find(fileQuery).into(es);
            for (GridFSFile e : es) {
                gridFS.delete(e.getObjectId());
            }
        } catch (Exception e) {
            log.error("failed to delete old gridfsfile", e);
        }
        gridFS.uploadFromStream(getId() + fileid, new ByteArrayInputStream(data));
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
