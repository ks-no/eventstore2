package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import no.ks.eventstore2.Event;

import java.io.ByteArrayOutputStream;

public abstract class AbstractJournalStorage implements JournalStorage {

    private final ThreadLocal<Kryo> kryoThread = new ThreadLocal<>();
    private KryoClassRegistration kryoClassRegistration;

    public AbstractJournalStorage(KryoClassRegistration kryoClassRegistration) {
        this.kryoClassRegistration = kryoClassRegistration;
    }

    public abstract void saveEvent(Event event);

    public abstract boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent);

    public abstract boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey);

    public abstract void open();

    public abstract void close();

    public abstract void upgradeFromOldStorage(String aggregateType);

    public abstract void doBackup(String backupDirectory, String backupfilename);

    public abstract EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId);

    protected ByteArrayOutputStream createByteArrayOutputStream(final Event event) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
        getKryo().writeClassAndObject(kryodata, event);
        kryodata.close();
        return output;
    }

    public Kryo getKryo(){
        if(kryoThread.get() == null){
            EventStoreKryo kryo = new EventStoreKryo();
            kryoClassRegistration.registerClasses(kryo);
            kryoThread.set(kryo);
        }
        return kryoThread.get();
    }
}
