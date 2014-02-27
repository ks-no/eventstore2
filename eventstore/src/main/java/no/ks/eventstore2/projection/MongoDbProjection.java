package no.ks.eventstore2.projection;


import akka.actor.ActorRef;
import com.mongodb.*;
import no.ks.eventstore2.store.MongoDbStore;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public abstract class MongoDbProjection extends ProjectionSnapshot {

    private MongoDbStore store;

    public MongoDbProjection(ActorRef eventStore, MongoClient client) throws Exception {
        super(eventStore);
        this.store = new MongoDbStore(client, "SnapshotRepository");
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
                        .append("data", serializeData())
                        .append("dataVersion", getSnapshotDataVersion())
                        .append("projectionId", getClass().getSimpleName())
                        .append("hostname", getHostName());

                collection.save(update);
                log.info("{} Saved snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);
            }
        } catch (Exception e) {
            log.error("Failed to write snapshot", e);
        }
    }

    @Override
    public void loadSnapshot() {
        log.info("{} loading snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);

        try {
            store.open();
            DBCollection collection = store.getCollection("snapshot");
            BasicDBObject query = new BasicDBObject("projectionId", getClass().getSimpleName())
                    .append("dataVersion", getSnapshotDataVersion())
                    .append("hostname", getHostName());

            DBCursor dbObjects = collection.find(query);

            if (dbObjects.hasNext()) {
                DBObject next = dbObjects.next();
                byte[] data = (byte[]) next.get("data");
                if (data != null) {
                    deSerializeData(data);
                }
                String latestJournalIdSnapshoted = (String)next.get("jid");
                if (latestJournalIdSnapshoted != null) {
                    latestJournalidReceived = latestJournalIdSnapshoted;
                    log.info("loaded snapshot for event {}", latestJournalidReceived);
                }
            }
        } catch (Exception e ) {
            log.error("Failed to load snapshot for {}", getClass().getSimpleName(), e);
            latestJournalidReceived = null;
        }
    }

    protected String getHostName() {
        try {
        return InetAddress.getLocalHost().getHostName();
        }catch (UnknownHostException e) {
            return "unknown";
        }
    }

    private String getId() {
        return "id_" + getSnapshotDataVersion() + "_" + this.getClass().getName();
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
