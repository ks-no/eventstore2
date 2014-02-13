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
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class LevelDbJournalStorage implements JournalStorage {

    private final String directory;
    Kryo kryo = new Kryo();
    private Logger log = LoggerFactory.getLogger(LevelDbJournalStorage.class);
    private static String currentDataVersion = "01";
    private long eventReadLimit = 1000L;;
    private LevelDbStore levelDbStore;

    public LevelDbJournalStorage(String directory, KryoClassRegistration registration) {
        levelDbStore = new LevelDbStore(directory, 100);
        this.directory = directory;
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        registration.registerClasses(kryo);
    }

    public LevelDbJournalStorage(String directory, KryoClassRegistration registration, long eventReadLimit) {
        this(directory,registration);
        this.eventReadLimit = eventReadLimit;
    }

    public void upgradeFromOldStorage(String aggregateId, JournalStorage storage){
        if(levelDbStore.getDb() == null) throw new RuntimeException("Database not open, please open first");
        String upgradedToVersionKey = "!sys!upgradedaggregate!" + currentDataVersion + "!" + aggregateId;
        byte[] key = bytes(upgradedToVersionKey + aggregateId);
        String string = asString(levelDbStore.getDb().get(key));
        if(!"true".equals(string)){
            log.info("Reading events for aggregate " + aggregateId + " from old storage");
            storage.loadEventsAndHandle(aggregateId, new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    saveEvent(event);
                }
            });
            levelDbStore.getDb().put(key, bytes(String.valueOf(true)));
            log.info("Events for aggregate " + aggregateId + " upgraded");
        }
    }

    public void open() {
        levelDbStore.openDb();
    }

    @Override
    public void saveEvent(Event event) {
        log.trace("Saving event " + event);
        String aggregateId = event.getAggregateId();
        long key = getNextAvailableKeyForAggregate(aggregateId);
        String journalid = convertToStringKey(key);
        String leveldbhashkey = getKey(event.getAggregateId(), journalid);
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
        byte[] bytes = outputs.toByteArray();
        return bytes;
    }

    String getKey(String aggregateId, String journalid) {
        return aggregateId + "!" + journalid;
    }

    long getNextAvailableKeyForAggregate(String aggregateId) {
        DBIterator iterator = levelDbStore.getDb().iterator();
        try {
            iterator.seekToLast();
            if (!iterator.hasPrev() && !iterator.hasNext()) {
                //empte db
                return 0L;
            }
            iterator.seek(bytes(aggregateId + "~"));
            if (!iterator.hasPrev()) {
                // maby last aggregate in db
                iterator.seekToLast();
                if (iterator.hasNext()) {
                    String key = asString(iterator.next().getKey());
                    if (key.startsWith(aggregateId))
                        return getNextKeyFromKey(key);
                }
            }
            if (!iterator.hasPrev() && !iterator.hasNext()) {
                // aggregate not found in db
                return 0L;
            }
            String key = asString(iterator.prev().getKey());
            if (!key.startsWith(aggregateId)) {
                // no key containging aggregate, new aggregate
                return 0L;
            }
            return getNextKeyFromKey(key);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private long getNextKeyFromKey(String key) {
        return Long.parseLong(key.substring(key.indexOf("!") + 1)) + 1L;
    }

    public void printDB() throws IOException {
        levelDbStore.printDB();
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateid, handleEvent, convertToStringKey(0), eventReadLimit);
    }

    private boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent, String fromkey, long limit){
        DBIterator iterator = levelDbStore.getDb().iterator();
        iterator.seek(bytes(aggregateid + "!" + fromkey));
        long count=0L;
        while (iterator.hasNext() && count < limit) {
            Map.Entry<byte[], byte[]> next = iterator.next();
            String key = asString(next.getKey());
            if (key.startsWith(aggregateid)) {
                handleEvent.handleEvent(deSerialize(next.getValue()));
                count++;
            }
        }
        return (count < limit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateid, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateid, handleEvent, convertToStringKey(getNextKeyFromKey(fromKey)), eventReadLimit);
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value), 4000);
        return (Event) kryo.readClassAndObject(input);
    }

    public void close() {
       levelDbStore.close();
    }
}
