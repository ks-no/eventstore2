package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.store.LevelDbStore;
import org.iq80.leveldb.DBIterator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class LevelDbJournalStorage implements JournalStorage {

    private Kryo kryo = new Kryo();
    private Logger log = LoggerFactory.getLogger(LevelDbJournalStorage.class);
    private static String currentDataVersion = "01";
    private long eventReadLimit = 1000L;
    private LevelDbStore levelDbStore;

    public LevelDbJournalStorage(String directory, KryoClassRegistration registration) {
        levelDbStore = new LevelDbStore(directory, 100);
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        registration.registerClasses(kryo);
    }

    public LevelDbJournalStorage(String directory, KryoClassRegistration registration, long eventReadLimit) {
        this(directory,registration);
        this.eventReadLimit = eventReadLimit;
    }

    public void upgradeFromOldStorage(String aggregateType, JournalStorage storage){
        if(levelDbStore.getDb() == null) {
        	throw new RuntimeException("Database not open, please open first");
        }
        String upgradedToVersionKey = "!sys!upgradedaggregate!" + currentDataVersion + "!" + aggregateType;
        byte[] key = bytes(upgradedToVersionKey + aggregateType);
        String string = asString(levelDbStore.getDb().get(key));
        if(!"true".equals(string)){
            log.info("Reading events for aggregate " + aggregateType + " from old storage");
            storage.loadEventsAndHandle(aggregateType, new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    saveEvent(event);
                }
            });
            levelDbStore.getDb().put(key, bytes(String.valueOf(true)));
            log.info("Events for aggregate " + aggregateType + " upgraded");
        }
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {
        levelDbStore.doBackup(backupDirectory, backupfilename);
    }

    @Override
    public EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId) {
        return null;
    }

    public void open() {
        levelDbStore.open();
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void saveEvent(Event event) {
        log.trace("Saving event " + event);
        long key = getNextAvailableKeyForAggregate(event.getAggregateType());
        String journalid = convertToStringKey(key);
        String leveldbhashkey = getKey(event.getAggregateType(), journalid);
        log.trace("got key" + leveldbhashkey);
        event.setJournalid(journalid);
        levelDbStore.getDb().put(bytes(leveldbhashkey), serielize(event));
    }

    private String convertToStringKey(long key) {
        return String.format("%019d", key);
    }

    private byte[] serielize(Event event) {
        final ByteArrayOutputStream outputs = new ByteArrayOutputStream();
        ByteBufferOutput output = new ByteBufferOutput(outputs);
        kryo.writeClassAndObject(output, event);
        output.close();
        return outputs.toByteArray();
    }

    private String getKey(String aggregateType, String journalid) {
        return aggregateType + "!" + journalid;
    }

    protected long getNextAvailableKeyForAggregate(String aggregateType) {
        DBIterator iterator = levelDbStore.getDb().iterator();
        try {
            iterator.seekToLast();
            if (!iterator.hasPrev() && !iterator.hasNext()) {
                //empte db
                return 0L;
            }
            iterator.seek(bytes(aggregateType + "~"));
            if (!iterator.hasPrev()) {
                // maby last aggregate in db
                iterator.seekToLast();
                if (iterator.hasNext()) {
                    String key = asString(iterator.next().getKey());
                    if (key.startsWith(aggregateType)) {
                    	return getNextKeyFromKey(key);
                    }
                }
            }
            if (!iterator.hasPrev() && !iterator.hasNext()) {
                // aggregate not found in db
                return 0L;
            }
            String key = asString(iterator.prev().getKey());
            if (!key.startsWith(aggregateType)) {
                // no key containging aggregate, new aggregate
                return 0L;
            }
            return getNextKeyFromKey(key);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
            	log.info("Error closing iterator", e);
            }
        }
    }

    private long getNextKeyFromKey(String key) {
        return Long.parseLong(key.substring(key.indexOf('!') + 1)) + 1L;
    }

    public void printDB() throws IOException {
        levelDbStore.printDB();
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, convertToStringKey(0), eventReadLimit);
    }

    private boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromkey, long limit){
        DBIterator iterator = levelDbStore.getDb().iterator();
        iterator.seek(bytes(aggregateType + "!" + fromkey));
        long count=0L;
        while (iterator.hasNext() && count < limit) {
            Map.Entry<byte[], byte[]> next = iterator.next();
            String key = asString(next.getKey());
            if (key.startsWith(aggregateType)) {
                handleEvent.handleEvent(deSerialize(next.getValue()));
                count++;
            }
        }
        return (count < limit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateType, handleEvent, convertToStringKey(getNextKeyFromKey(fromKey)), eventReadLimit);
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value), 4000);
        return (Event) kryo.readClassAndObject(input);
    }

    public void close() {
       levelDbStore.close();
    }


}
