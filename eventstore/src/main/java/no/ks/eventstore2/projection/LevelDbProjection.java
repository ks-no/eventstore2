package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.eventstore2.store.LevelDbStore;
import org.iq80.leveldb.WriteBatch;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public abstract class LevelDbProjection extends ProjectionSnapshot {

    private String snapshotDirectory;
    private LevelDbStore store;

    public LevelDbProjection(ActorRef eventStore, String snapshotDirectory) {
        super(eventStore);
        this.snapshotDirectory = snapshotDirectory;
    }

    protected LevelDbProjection(ActorRef eventStore) {
        super(eventStore);
        snapshotDirectory = System.getProperty("java.io.tmpdir" + File.separator + "snapshots" + File.separator + this.getClass().getSimpleName());
    }

    public void setSnapshotDirectory(String snapshotDirectory) {
        this.snapshotDirectory = snapshotDirectory;
    }

    @Override
    public void preStart() {
        new File(snapshotDirectory).mkdirs();
        store = new LevelDbStore(snapshotDirectory,10);
        super.preStart();
    }

    @Override
    public void saveSnapshot() {
        log.info("{} Saving snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);
        WriteBatch writeBatch = null;
        try {
            store.open();

            writeBatch = store.getDb().createWriteBatch();
            if (latestJournalidReceived != null) {
                writeBatch.put(getDataKey(), serializeData());
                writeBatch.put(getLatestEventIdKey(), bytes(latestJournalidReceived));
                store.getDb().write(writeBatch);
                log.info("{} Saved snapshot for event {}", getClass().getSimpleName(), latestJournalidReceived);
            }
        } finally {
            if (writeBatch != null) {
            	try {
            		writeBatch.close();
	            } catch (IOException e) {
	                log.error("Failed to write snapshot", e);
	            }
            }
            store.close();
        }
    }

    protected abstract byte[] serializeData();

    /**
     * Data version this projection uses
     * @return
     */
    protected abstract String getSnapshotDataVersion();

    private byte[] getDataKey() {
        return bytes("snapshot!" + getSnapshotDataVersion() + "!" + this.getClass().getName() + "!data");
    }
    private byte[] getLatestEventIdKey() {
        return bytes("snapshot!" + getSnapshotDataVersion() + "!" + this.getClass().getName() + "!key");
    }

    @Override
    public void loadSnapshot() {
        try {
            store.open();
            byte[] data = store.getDb().get(getDataKey());
            if(data != null)
                deSerializeData(data);
            byte[] latestJournalIdSnapshoted = store.getDb().get(getLatestEventIdKey());
            if(latestJournalIdSnapshoted != null){
                latestJournalidReceived = asString(latestJournalIdSnapshoted);
                log.info("loaded snapshot for event {}", latestJournalidReceived);
            }
        } catch(Exception e){
            log.error("Failed to load snapshot for {}", snapshotDirectory, e);
            latestJournalidReceived = null;
        } finally {
            store.close();
        }

    }

    protected abstract void deSerializeData(byte[] bytes);
}
