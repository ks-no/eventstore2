package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import no.ks.eventstore2.store.MongoDbStore;
import org.apache.commons.io.IOUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public abstract class MongoDbProjection extends ProjectionSnapshot {

    private MongoDbStore store;
    private GridFS gridFS;

    public MongoDbProjection(ActorRef eventStore, MongoClient client) throws Exception {
        super(eventStore);
        this.store = new MongoDbStore(client, "SnapshotRepository");
        gridFS = new GridFS(store.getDb(), "snapshot_data");
    }

    @Override
    public void preStart() {
        super.preStart();

    }

    @Override
    public void saveSnapshot() {
        log.info("{} Saving snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);

        try {
            store.open();
            DBCollection collection = store.getCollection("snapshot");
            if (latestJournalidReceived != null) {
                DBObject update = new BasicDBObject("_id", getId())
                        .append("jid", latestJournalidReceived)
                        .append("dataVersion", getSnapshotDataVersion())
                        .append("projectionId", getClass().getSimpleName())
                        .append("hostname", getHostName());

                collection.save(update);

                saveDataToGridFS();

                log.info("{} Saved snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);
            }
        } catch (Exception e) {
            log.error("Failed to write snapshot", e);
        }
    }


    @Override
    public void loadSnapshot() {
        log.info("{} loading snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);

        DBCursor dbObjects = null;
        try {
            store.open();
            DBCollection collection = store.getCollection("snapshot");
            BasicDBObject query = new BasicDBObject("projectionId", getClass().getSimpleName())
                    .append("dataVersion", getSnapshotDataVersion())
                    .append("hostname", getHostName());

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

    protected void saveDataToGridFS() {
        gridFS.remove(new BasicDBObject("_id", getId()));

        GridFSInputFile file = gridFS.createFile(serializeData());
        file.setId(getId());
        file.save();
    }

    protected String getHostName() {
        try {
        return InetAddress.getLocalHost().getHostName();
        }catch (UnknownHostException e) {
            return "unknown";
        }
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
