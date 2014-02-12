package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.*;

public class LevelDbJournalStorage implements JournalStorage {

    private final String directory;
    private DB db;
    Kryo kryo = new Kryo();
    private Logger log = LoggerFactory.getLogger(LevelDbJournalStorage.class);
    private static String currentDataVersion = "01";

    public LevelDbJournalStorage(String directory, KryoClassRegistration registration) {
        this.directory = directory;
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        registration.registerClasses(kryo);
    }

    public void upgradeFromOldStorage(String aggregateId, JournalStorage storage){
        if(db == null) throw new RuntimeException("Database not open, please open first");
        String upgradedToVersionKey = "!sys!upgradedaggregate!" + currentDataVersion + "!" + aggregateId;
        byte[] key = bytes(upgradedToVersionKey + aggregateId);
        String string = asString(db.get(key));
        if(!"true".equals(string)){
            log.info("Reading events for aggregate " + aggregateId + " from old storage");
            storage.loadEventsAndHandle(aggregateId, new HandleEvent() {
                @Override
                public void handleEvent(Event event) {
                    saveEvent(event);
                }
            });
            db.put(key,bytes(String.valueOf(true)));
            log.info("Events for aggregate " + aggregateId + " upgraded");
        }
    }

    public void open() {
        openDb();
    }

    private void openDb() {
        if (db == null) {
            Options options = new Options();
            options.cacheSize(100 * 1048576); // 100MB cache
            options.createIfMissing(true);
            if(!new File(directory).exists())
                new File(directory).mkdirs();
            File lockfile = new File(directory + File.pathSeparator + "LOCK");
            if(lockfile.exists()){
                log.warn("LEVELDB: Lockfile exists, waiting 5 sec");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) { }
                if(lockfile.exists()){
                    log.warn("LEVELDB: Deleteing lockfile");
                    lockfile.delete();
                }
            }
            try {
                db = factory.open(new File(directory), options);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void saveEvent(Event event) {
        log.debug("Saving event " + event);
        String aggregateId = event.getAggregateId();
        long key = getNextKey(aggregateId);
        String key1 = getKey(event.getAggregateId(), key);
        log.debug("got key" + key1);
        db.put(bytes(key1), serielize(event));
    }

    private byte[] serielize(Event event) {
        final ByteArrayOutputStream outputs = new ByteArrayOutputStream();
        ByteBufferOutput output = new ByteBufferOutput(outputs);
        kryo.writeClassAndObject(output, event);
        output.close();
        byte[] bytes = outputs.toByteArray();
        return bytes;
    }

    String getKey(String aggregateId, long key) {
        return aggregateId + "!" + String.format("%019d", key);
    }

    long getNextKey(String aggregateId) {
        DBIterator iterator = db.iterator();
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
        DBIterator iterator = db.iterator();
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                java.lang.System.out.println(key + " = " + value);
            }
        } finally {
            // Make sure you close the iterator to avoid resource leaks.
            iterator.close();
        }
    }

    @Override
    public void loadEventsAndHandle(String aggregateid, HandleEvent handleEvent) {
        DBIterator iterator = db.iterator();
        iterator.seekToFirst();
        iterator.seek(bytes(aggregateid + "!"));
        while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> next = iterator.next();
            String key = asString(next.getKey());
            if (key.startsWith(aggregateid)) {
                handleEvent.handleEvent(deSerialize(next.getValue()));
            }
        }
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value), 4000);
        return (Event) kryo.readClassAndObject(input);
    }

    public void close() {
        try {
            if(db != null){
                db.close();
                db = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
