package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import no.ks.eventstore2.TakeSnapshot;
import no.ks.eventstore2.store.LevelDbStore;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public abstract class LevelDbProjection extends ProjectionSnapshot {

    private final String snapshotDirectory;
    private LevelDbStore store;

    public LevelDbProjection(ActorRef eventStore, String snapshotDirectory) {
        super(eventStore);
        this.snapshotDirectory = snapshotDirectory;
    }

    private final Cancellable snapshotSchedule = getContext().system().scheduler().schedule(
            Duration.create(1, TimeUnit.HOURS),
            Duration.create(2, TimeUnit.HOURS),
            getSelf(), new TakeSnapshot(), getContext().dispatcher(), null);

    @Override
    public void postStop() {
        snapshotSchedule.cancel();
    }

    @Override
    public void preStart() {
        store = new LevelDbStore(snapshotDirectory,10);
        super.preStart();
    }

    @Override
    public void saveSnapshot() {
        try {
            store.open();
            if(latestJournalidReceived != null){
                store.getDb().put(getDataKey(), serializeData());
                store.getDb().put(getLatestEventIdKey(),bytes(latestJournalidReceived));
            }
        } finally {
            store.close();
        }
    }

    protected abstract byte[] serializeData();

    private byte[] getDataKey() {
        return bytes("snapshot!" + this.getClass().getName() + "!data");
    }
    private byte[] getLatestEventIdKey() {
        return bytes("snapshot!" + this.getClass().getName() + "!key");
    }

    @Override
    public void loadSnapshot() {
        try {
            store.open();
            byte[] data = store.getDb().get(getDataKey());
            if(data != null)
                deSerializeData(data);
            byte[] latestJournalIdSnapshoted = store.getDb().get(getLatestEventIdKey());
            if(latestJournalIdSnapshoted != null)
                latestJournalidReceived = asString(latestJournalIdSnapshoted);
        } finally {
            store.close();
        }
    }

    protected abstract void deSerializeData(byte[] bytes);
}
