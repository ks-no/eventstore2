package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.mongodb.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import org.joda.time.DateTime;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class MongoDBJournal implements JournalStorage {
    private final ThreadLocal<Kryo> tlkryo = new ThreadLocal<>();
    private final DBCollection metaCollection;
    private DB db;
    private final KryoClassRegistration registration;

    private String dataversion = "01";

    private int eventReadLimit = 5000;
    private Logger log = LoggerFactory.getLogger(MongoDBJournal.class);

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates, int eventReadLimit) {
        this(db, registration, aggregates);
        this.eventReadLimit = eventReadLimit;
    }

    public MongoDBJournal(DB db, KryoClassRegistration registration, List<String> aggregates) {
        this.db = db;
        this.registration = registration;
        db.setWriteConcern(WriteConcern.SAFE);
        for (String aggregate : aggregates) {
            db.getCollection(aggregate).ensureIndex("jid");
            db.getCollection(aggregate).ensureIndex("rid");
            db.getCollection(aggregate).setWriteConcern(WriteConcern.SAFE);
        }
        metaCollection = db.getCollection("journalMetadata");
        metaCollection.setWriteConcern(WriteConcern.SAFE);


    }

    public Kryo getKryo(){
        if(tlkryo.get() == null){
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
            kryo.register(DateTime.class, new JodaDateTimeSerializer());
            registration.registerClasses(kryo);
            tlkryo.set(kryo);
        }
        return tlkryo.get();
    }

    private byte[] serielize(Event event) {
        final ByteArrayOutputStream outputs = new ByteArrayOutputStream();
        ByteBufferOutput output = new ByteBufferOutput(outputs);
        getKryo().writeClassAndObject(output, event);
        output.close();
        return outputs.toByteArray();
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value));
        return (Event) getKryo().readClassAndObject(input);
    }

    @Override
    public void saveEvent(Event event) {
        final DBCollection collection = db.getCollection(event.getAggregateType());
        event.setJournalid(String.valueOf(getNextJournalId(collection)));
        final BasicDBObject doc = new BasicDBObject("jid", Long.parseLong(event.getJournalid())).
                append("rid", event.getAggregateRootId()).
                append("d", serielize(event));

        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                collection.insert(doc);
                return null;
            }
        });
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        if(events == null || events.size() == 0) {
        	return;
        }
        String agg = events.get(0).getAggregateType();
        final DBCollection collection = db.getCollection(agg);
        long nextJournalId = getNextJournalId(collection);
        final List<DBObject> dbObjectArrayList = new ArrayList<DBObject>();
        for (Event event : events) {
            event.setJournalid(String.valueOf(nextJournalId));
            BasicDBObject doc = new BasicDBObject("jid", nextJournalId).
                    append("rid", event.getAggregateRootId()).
                    append("d", serielize(event));
            nextJournalId++;
            dbObjectArrayList.add(doc);
        }

        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                collection.insert(dbObjectArrayList);
                return null;
            }
        });

    }

    private long getNextJournalId(final DBCollection collection) {
        long count = 0L;

        count = MongoDbOperations.doDbOperation(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return collection.getCount();
            }
        });

        return count + 1;
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, "0", eventReadLimit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateType, handleEvent, fromKey, eventReadLimit);
    }

    private boolean loadEventsAndHandle(final String aggregateType, HandleEvent handleEvent, String fromKey, final int readlimit) {
        final BasicDBObject query = new BasicDBObject("jid", new BasicDBObject("$gt", Long.parseLong(fromKey)));
        final DBCursor dbObjects = MongoDbOperations.doDbOperation(new Callable<DBCursor>() {
            @Override
            public DBCursor call() throws Exception {
                return db.getCollection(aggregateType).find(query).sort(new BasicDBObject("jid", 1)).limit(readlimit);
            }
        });
        int i = 0;
        try {
            while (MongoDbOperations.doDbOperation(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return dbObjects.hasNext();
                }
            })) {
                DBObject next;

                next = MongoDbOperations.doDbOperation(new Callable<DBObject>() {
                    @Override
                    public DBObject call() throws Exception {
                        return dbObjects.next();
                    }
                });
                final Event event = deSerialize((byte[]) next.get("d"));
                event.setVersion(-1);
                handleEvent.handleEvent(event);
                i++;
            }
        } finally {
            dbObjects.close();
        }
        return i < readlimit;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage) {
        DBCursor id = metaCollection.find(new BasicDBObject("_id", "upgrade_" + aggregateType));
        if (id.hasNext() && dataversion.equals(id.next().get("version"))) {
            log.info("Already upgraded aggregate " + aggregateType);
            return;
        }

        final ArrayList<Event> events = new ArrayList<Event>();
        boolean done = false;
        HandleEvent handleEvent = new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
                if(events.size() > eventReadLimit){
                    log.info("Events reached {}, saving away", eventReadLimit);
                    saveEvents(events);
                    events.clear();
                }
            }
        };
        done = oldStorage.loadEventsAndHandle(aggregateType, handleEvent);
        while (!done) {
            String lastJournalID = events.get(events.size()-1).getJournalid();
            log.info("saving to lastJournalId {}",lastJournalID);
            saveEvents(events);
            events.clear();
            done = oldStorage.loadEventsAndHandle(aggregateType, handleEvent,lastJournalID);

        }
        saveEvents(events);
        events.clear();
        metaCollection.save(new BasicDBObject("_id", "upgrade_" + aggregateType).append("version", dataversion));
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {

    }

    @Override
    public EventBatch loadEventsForAggregateId(final String aggregateType, String aggregateId, String fromJournalId) {
        final BasicDBObject query = new BasicDBObject("rid",  aggregateId);
        if(fromJournalId != null)
            query.append("jid", new BasicDBObject("$gt", Long.parseLong(fromJournalId)));

        final DBCursor dbObjects = MongoDbOperations.doDbOperation(new Callable<DBCursor>() {
            @Override
            public DBCursor call() throws Exception {
                return db.getCollection(aggregateType).find(query).sort(new BasicDBObject("jid", 1)).limit(eventReadLimit);
            }
        });
        List<Event> events = new ArrayList<>();
        try {
            while (dbObjects.hasNext()) {
                DBObject next = MongoDbOperations.doDbOperation(new Callable<DBObject>() {
                    @Override
                    public DBObject call() throws Exception {
                        return dbObjects.next();
                    }
                });
                events.add(deSerialize((byte[]) next.get("d")));
            }
        } finally {
            dbObjects.close();
        }
        return new EventBatch(aggregateType, aggregateId, events, events.size() != eventReadLimit);
    }


}
