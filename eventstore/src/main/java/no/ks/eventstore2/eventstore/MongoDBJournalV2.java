package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import com.mongodb.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import no.ks.eventstore2.Event;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

public class MongoDBJournalV2 implements JournalStorage {
    private final ThreadLocal<Kryo> tlkryo = new ThreadLocal<>();
    private final DBCollection metaCollection;
    private final DBCollection counters;
    private DB db;
    private final KryoClassRegistration registration;
    private HashSet<String> aggregates;

    private String dataversion = "02";

    private int eventReadLimit = 5000;
    private Logger log = LoggerFactory.getLogger(MongoDBJournalV2.class);


    public MongoDBJournalV2(DB db, KryoClassRegistration registration, List<String> aggregates, int eventReadLimit) {
        this(db, registration, aggregates);
        this.eventReadLimit = eventReadLimit;
    }

    public MongoDBJournalV2(DB db, KryoClassRegistration registration, List<String> aggregates) {
        this.db = db;
        this.registration = registration;
        this.aggregates = new HashSet<>(aggregates);
        metaCollection = db.getCollection("journalMetadata");
        metaCollection.setWriteConcern(WriteConcern.SAFE);
        db.setWriteConcern(WriteConcern.SAFE);
        for (String aggregate : aggregates) {
            db.getCollection(aggregate).createIndex(new BasicDBObject("jid",1),new BasicDBObject("unique",true));
            db.getCollection(aggregate).createIndex(new BasicDBObject("rid",1));
            db.getCollection(aggregate).createIndex(new BasicDBObject("rid",1).append("v",1),new BasicDBObject("unique",true));
            db.getCollection(aggregate).setWriteConcern(WriteConcern.SAFE);
        }

        counters = db.getCollection("counters");

    }

    public Kryo getKryo() {
        if (tlkryo.get() == null) {
            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
            kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
            kryo.register(DateTime.class, new JodaDateTimeSerializer());
            kryo.setRegistrationRequired(true);
            registration.registerClasses(kryo);
            tlkryo.set(kryo);
        }
        return tlkryo.get();
    }

    private byte[] serielize(Event event) {
        final ByteArrayOutputStream outputs = new ByteArrayOutputStream();
        Output output = new Output(outputs);
        getKryo().writeClassAndObject(output, event);
        output.close();
        return outputs.toByteArray();
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(new ByteArrayInputStream(value));
        return (Event) getKryo().readClassAndObject(input);
    }

    @Override
    public void saveEvent(final Event event) {
        if(!aggregates.contains(event.getAggregateType())) throw new RuntimeException("Aggregate " + event.getAggregateType() + " not registered");
        final DBCollection collection = db.getCollection(event.getAggregateType());
        final int journalid = getNextValueInSeq("journalid_" + event.getAggregateType(), 1);
        event.setJournalid(String.valueOf(journalid));
        // if version is not set, find the next one
        if(event.getVersion()  == -1){
            event.setVersion(getNextVersion(collection, event));
        }
        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                WriteResult insert = collection.insert(getEventDBObject(event, journalid));
                return null;
            }
        }, 3, 500);
    }

    private BasicDBObject getEventDBObject(Event event, int journalid) {
        return new BasicDBObject("jid", journalid).
                    append("rid", event.getAggregateRootId()).
                    append("v", event.getVersion()).
                    append("d", serielize(event));
    }

    private int getNextVersion(DBCollection collection, Event event) {
        DBObject one = collection.find(new BasicDBObject("rid", event.getAggregateRootId()), new BasicDBObject("v",1)).sort(new BasicDBObject("v", -1)).limit(1).one();
        if(one == null) return 0;
        return (Integer) one.get("v") +1;
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        if (events == null || events.size() == 0) {
            return;
        }
        String agg = events.get(0).getAggregateType();
        if(!aggregates.contains(agg)) throw new RuntimeException("Aggregate "+ agg + " not registered");
        final DBCollection collection = db.getCollection(agg);

        final List<DBObject> dbObjectArrayList = new ArrayList<DBObject>();
        int maxJournalId = getNextValueInSeq("journalid_" + agg, events.size());
        int jid = (maxJournalId - events.size())+1;
        final HashMap<String, Integer> versions_for_aggregates = new HashMap<String, Integer>();
        for (Event event : events) {
            event.setJournalid(String.valueOf(jid));
            // if version is not set, find the next one
            if(event.getVersion()  == -1){
                if(versions_for_aggregates.containsKey(event.getAggregateRootId())){
                    final int version = versions_for_aggregates.get(event.getAggregateRootId()) + 1;
                    event.setVersion(version);
                    versions_for_aggregates.put(event.getAggregateRootId(), version);
                } else {
                    event.setVersion(getNextVersion(collection, event));
                    versions_for_aggregates.put(event.getAggregateRootId(), event.getVersion());
                }
            log.debug("Saving event " + event);
            }
            dbObjectArrayList.add(getEventDBObject(event, jid));
            jid++;
        }

        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                log.debug("Saving " + dbObjectArrayList);
                collection.insert(dbObjectArrayList);
                return null;
            }
        }, 0, 500);

    }


    private int getNextValueInSeq(final String counterName, final int numbers) {
        return MongoDbOperations.doDbOperation(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                DBObject andModify = counters.findAndModify(new BasicDBObject("_id", counterName), null, null, false, new BasicDBObject("$inc", new BasicDBObject("seq", numbers)), true, true);
                return (Integer) andModify.get("seq");
            }
        });
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, "0", eventReadLimit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateType, handleEvent, fromKey, eventReadLimit);
    }

    boolean loadEventsAndHandle(final String aggregateType, HandleEvent handleEvent, String fromKey, final int readlimit) {
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
                if(!(""+next.get("jid")).equals(event.getJournalid())){
                    log.error("Journalid in database dosen't match event db: {} event: {} : completeevent:{}", next.get("jid"), event.getJournalid(), event);
                }
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
            }
        };
        String lastJournalID = "0";
        do  {
            events.clear();
            done = oldStorage.loadEventsAndHandle(aggregateType, handleEvent, lastJournalID);
            if(events.size()> 0) {
                lastJournalID = events.get(events.size() - 1).getJournalid();
                log.info("saving to lastJournalId {}", lastJournalID);
                saveEvents(events);
            }

        } while(!done);

        metaCollection.save(new BasicDBObject("_id", "upgrade_" + aggregateType).append("version", dataversion));
        events.clear();
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {

    }

    @Override
    public EventBatch loadEventsForAggregateId(final String aggregateType, String aggregateId, String fromJournalId) {
        final BasicDBObject query = new BasicDBObject("rid", aggregateId);
        if (fromJournalId != null)
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
