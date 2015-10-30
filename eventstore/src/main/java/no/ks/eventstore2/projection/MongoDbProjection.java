package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import no.ks.eventstore2.store.MongoDbStore;
import org.apache.commons.io.IOUtils;

public abstract class MongoDbProjection extends ProjectionSnapshot {

    private MongoDbStore store;
    private GridFS gridFS;

    private static String nodename = System.getProperty("nodename") != null ? System.getProperty("nodename") : "local";

    public MongoDbProjection(ActorRef eventStore, MongoClient client) {
        super(eventStore);
        this.store = new MongoDbStore(client, nodename + "_SnapshotRepository");
        gridFS = new GridFS(store.getDb(), nodename + "_snapshot_data");
        store.getCollection("snapshot").createIndex(new BasicDBObject("dataVersion", 1).append("projectionId", 1));
    }

    @Override
    public void preStart() {
        super.preStart();
        loadSnapshot();

    }

    @Override
    public void saveSnapshot() {
        final String simpleName = getClass().getSimpleName();
        log.info("{} Saving snapshot for event {}", simpleName, latestJournalidReceived);

        final byte[] data = serializeData();
        log.info("{} serialized data, storing to db", simpleName);
        context().dispatcher().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    DBCollection collection = store.getCollection("snapshot");
                    if (latestJournalidReceived != null) {
                        DBObject update = new BasicDBObject("_id", getId())
                                .append("jid", latestJournalidReceived)
                                .append("dataVersion", getSnapshotDataVersion())
                                .append("projectionId", simpleName);

                        collection.save(update);

                        saveDataToGridFS(data);

                        log.info("{} Saved snapshot for event {}", simpleName, latestJournalidReceived);
                    }
                } catch (Exception e) {
                    log.error("Failed to write snapshot", e);
                }
            }
        });
    }


    @Override
    public void loadSnapshot() {
        log.info("{} loading snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);

        DBCursor dbObjects = null;
        try {
            DBCollection collection = store.getCollection("snapshot");
            BasicDBObject query = new BasicDBObject("projectionId", getClass().getSimpleName())
                    .append("dataVersion", getSnapshotDataVersion());


            dbObjects = collection.find(query);

            if (dbObjects.hasNext()) {
                DBObject next = dbObjects.next();
                String latestJournalIdSnapshoted = (String)next.get("jid");

                BasicDBObject fileQuery = new BasicDBObject("_id", getId());
                GridFSDBFile file = gridFS.findOne(fileQuery);
                if (file != null) {
                    deSerializeData(IOUtils.toByteArray(file.getInputStream()));
                }

                if (latestJournalIdSnapshoted != null) {
                    latestJournalidReceived = latestJournalIdSnapshoted;
                    log.info("loaded snapshot for event {}", latestJournalidReceived);
                }
            }
        } catch (Exception e ) {
            log.error("Failed to load snapshot for {}", getClass().getSimpleName(), e);
            latestJournalidReceived = null;
        } finally {
            if (dbObjects != null) {
                dbObjects.close();
            }
        }
    }

    protected void saveDataToGridFS(byte[] data) {
        gridFS.remove(new BasicDBObject("_id", getId()));

        GridFSInputFile file = gridFS.createFile(data);
        file.setId(getId());
        file.save();
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
