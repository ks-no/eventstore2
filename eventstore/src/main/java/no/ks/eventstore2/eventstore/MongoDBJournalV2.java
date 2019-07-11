package no.ks.eventstore2.eventstore;

import akka.dispatch.Futures;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.shaded.org.objenesis.strategy.SerializingInstantiatorStrategy;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import eventstore.Messages;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.ProtobufHelper;
import org.bson.Document;
import org.bson.types.Binary;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MongoDBJournalV2 implements JournalStorage {
    private final ThreadLocal<Kryo> tlkryo = new ThreadLocal<>();
    private final MongoCollection<Document> metaCollection;
    private final MongoCollection<Document> counters;
    private MongoDatabase db;
    private com.mongodb.async.client.MongoDatabase dbasync;
    private final KryoClassRegistration registration;
    private HashSet<String> aggregates;

    private String dataversion = "02";

    private int eventReadLimit = 5000;
    private Logger log = LoggerFactory.getLogger(MongoDBJournalV2.class);


    public MongoDBJournalV2(MongoDatabase db, KryoClassRegistration registration, List<String> aggregates, int eventReadLimit, com.mongodb.async.client.MongoDatabase dbasync) {
        this(db, registration, aggregates, dbasync);
        this.eventReadLimit = eventReadLimit;
    }

    public MongoDBJournalV2(MongoDatabase db, KryoClassRegistration registration, List<String> aggregates, com.mongodb.async.client.MongoDatabase dbasync) {
        this.db = db;
        this.dbasync = dbasync;
        db.withWriteConcern(WriteConcern.JOURNALED);
        this.registration = registration;
        this.aggregates = new HashSet<>(aggregates);
        metaCollection = db.getCollection("journalMetadata");
        for (String aggregate : aggregates) {
            createIndexes(db, aggregate);
            db.getCollection(aggregate).withWriteConcern(WriteConcern.JOURNALED);
        }

        counters = db.getCollection("counters");

    }

    private void createIndexes(MongoDatabase db, String aggregate) {
        db.getCollection(aggregate).createIndex(new Document("jid",1), new IndexOptions().unique(true));
        db.getCollection(aggregate).createIndex(new Document("rid",1));
        db.getCollection(aggregate).createIndex(new Document("rid",1).append("v",1),new IndexOptions().unique(true));
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

    private Messages.EventWrapper deSerialize(Document d, String aggregateType) {
        try {
            return ProtobufHelper.newEventWrapper(d.getString("correlationid"),
                    d.getString("protoSerializationType"),
                    d.getString("rid"),
                    d.getLong("jid"),
                    aggregateType,
                    d.getLong("v"),
                    d.getLong("occuredon"),
                    (Binary) d.get("d"),
                    d.getString("created_by_user"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Event deSerialize(byte[] value) {
        Input input = new Input(value);
        return (Event) getKryo().readClassAndObject(input);
    }

    @Override
    public void saveEvent(final Event event) {
        if(!aggregates.contains(event.getAggregateType())) throw new RuntimeException("Aggregate " + event.getAggregateType() + " not registered");
        final MongoCollection<Document> collection = db.getCollection(event.getAggregateType());
        final int journalid = getNextValueInSeq("journalid_" + event.getAggregateType(), 1);
        event.setJournalid(String.valueOf(journalid));
        // if version is not set, find the next one
        if(event.getVersion()  == -1){
            event.setVersion(getNextVersion(collection, event.getAggregateRootId()));
        }
        MongoDbOperations.doDbOperation(() -> {collection.insertOne(getEventDBObject(event, journalid)); return null;}, 3, 500);
    }

    public Messages.EventWrapper saveEvent(Messages.EventWrapper eventWrapper) {
        if(!aggregates.contains(eventWrapper.getAggregateType())) throw new RuntimeException("Aggregate " + eventWrapper.getAggregateType() + " not registered");
        final MongoCollection<Document> collection = db.getCollection(eventWrapper.getAggregateType());
        final int journalid = getNextValueInSeq("journalidproto_" + eventWrapper.getAggregateType(), 1);
        // if version is not set, find the next one
        long version = eventWrapper.getVersion();
        if(version  == -1){
            version = getNextLongVersion(collection, eventWrapper.getAggregateRootId());
        }
        final Messages.EventWrapper eventWrapperWithversionAndJournalid = eventWrapper.toBuilder().setVersion(version).setJournalid(journalid).build();
        MongoDbOperations.doDbOperation(() -> {collection.insertOne(getEventDBObject(eventWrapperWithversionAndJournalid)); return null;}, 3, 500);
        return eventWrapperWithversionAndJournalid;
    }

    private Document getEventDBObject(Messages.EventWrapper eventWrapper) {
        return new Document("jid", eventWrapper.getJournalid())
                .append("rid", eventWrapper.getAggregateRootId())
                .append("v", eventWrapper.getVersion())
                .append("correlationid", eventWrapper.getCorrelationId())
                .append("occuredon", DateTime.now().getMillis())
                .append("protoSerializationType", eventWrapper.getProtoSerializationType())
                .append("d", eventWrapper.getEvent().toByteArray())
                .append("created_by_user", eventWrapper.getCreatedByUser());
    }

    private Document getEventDBObject(Event event, long journalid) {
        return new Document("jid", journalid).
                    append("rid", event.getAggregateRootId()).
                    append("v", event.getVersion()).
                    append("d", serielize(event));
    }

    private int getNextVersion(MongoCollection<Document> collection, String aggregateRootId) {
        FindIterable<Document> one = collection.find(new Document("rid", aggregateRootId)).sort(new Document("v", -1)).limit(1).projection(new Document("v",1));
        final Document first = one.first();
        if(first == null) return 0;
        return first.getInteger("v") +1;
    }


    private long getNextLongVersion(MongoCollection<Document> collection, String aggregateRootId) {
        FindIterable<Document> one = collection.find(new Document("rid", aggregateRootId)).sort(new Document("v", -1)).limit(1).projection(new Document("v",1));
        final Document first = one.first();
        if(first == null) return 0;
        return first.getLong("v") +1;
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        if (events == null || events.size() == 0) {
            return;
        }
        String agg = events.get(0).getAggregateType();
        if(!aggregates.contains(agg)) throw new RuntimeException("Aggregate "+ agg + " not registered");
        final MongoCollection<Document> collection = db.getCollection(agg);

        final List<Document> dbObjectArrayList = new ArrayList<>();
        int maxJournalId = getNextValueInSeq("journalid_" + agg, events.size());
        int jid = (maxJournalId - events.size())+1;
        final HashMap<String, Integer> versions_for_aggregates = new HashMap<>();
        for (Event event : events) {
            event.setJournalid(String.valueOf(jid));
            // if version is not set, find the next one
            if(event.getVersion()  == -1){
                if(versions_for_aggregates.containsKey(event.getAggregateRootId())){
                    final int version = versions_for_aggregates.get(event.getAggregateRootId()) + 1;
                    event.setVersion(version);
                    versions_for_aggregates.put(event.getAggregateRootId(), version);
                } else {
                    event.setVersion(getNextVersion(collection, event.getAggregateRootId()));
                }
                versions_for_aggregates.put(event.getAggregateRootId(), event.getVersion());
                log.debug("Saving event " + event);
            }
            dbObjectArrayList.add(getEventDBObject(event, jid));
            jid++;
        }

        MongoDbOperations.doDbOperation(() -> {
            log.debug("Saving " + dbObjectArrayList);
            collection.insertMany(dbObjectArrayList);
            return null;
        }, 0, 500);

    }

    @Override
    public List<Messages.EventWrapper> saveEventsBatch(List<Messages.EventWrapper> events) {
        if (events == null || events.size() == 0) {
            return Collections.emptyList();
        }
        List<Messages.EventWrapper> result = new ArrayList<>();
        String agg = events.get(0).getAggregateType();
        if(!aggregates.contains(agg)) throw new RuntimeException("Aggregate "+ agg + " not registered");
        final MongoCollection<Document> collection = db.getCollection(agg);

        final List<Document> dbObjectArrayList = new ArrayList<>();
        long maxJournalId = getNextValueInSeq("journalidproto_" + agg, events.size());
        long jid = (maxJournalId - events.size())+1;
        final HashMap<String, Long> versions_for_aggregates = new HashMap<>();
        long version = -1;
        for (Messages.EventWrapper event : events) {

            // if version is not set, find the next one
            if(event.getVersion()  == -1){
                if(versions_for_aggregates.containsKey(event.getAggregateRootId())){
                    version = versions_for_aggregates.get(event.getAggregateRootId()) + 1;
                    versions_for_aggregates.put(event.getAggregateRootId(), version);
                } else {
                    version = getNextLongVersion(collection, event.getAggregateRootId());
                }
                versions_for_aggregates.put(event.getAggregateRootId(), version);
                log.debug("Saving event {}",event);
            } else {
                version = event.getVersion();
            }
            final Messages.EventWrapper eventWrapperWithversionAndJournalid = event.toBuilder().setVersion(version).setJournalid(event.getJournalid() == 0 ? jid: event.getJournalid()).build();
            dbObjectArrayList.add(getEventDBObject(eventWrapperWithversionAndJournalid));
            result.add(eventWrapperWithversionAndJournalid);
            jid++;
        }

        MongoDbOperations.doDbOperation(() -> {
            log.debug("Saving {}",dbObjectArrayList);
            collection.insertMany(dbObjectArrayList);
            return null;
        }, 0, 500);
        return result;
    }

    private int getNextValueInSeq(final String counterName, final int numbers) {
        return MongoDbOperations.doDbOperation(() -> {
            Document andModify = counters.findOneAndUpdate(new Document("_id", counterName), new Document("$inc", new Document("seq", numbers)),new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER).projection(new Document("seq",1)));
            return (Integer) andModify.get("seq");
        });
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, "0", eventReadLimit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, 0, eventReadLimit);
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
        return loadEventsAndHandle(aggregateType, handleEvent, fromKey, eventReadLimit);
    }

    public boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata loadEvents, long journalid) {
        return loadEventsAndHandle(aggregateType, loadEvents, journalid, eventReadLimit);
    }

    public void upgradeToProto(String aggregateType) {
        Boolean upgraded = false;
        final MongoCursor<String> iterator = db.listCollectionNames().iterator();
        while(iterator.hasNext()){
            if((aggregateType + "_old").equals(iterator.next()))
                upgraded = true;
        }
        if(upgraded) {
            log.info("Aggregate {} already upgraded", aggregateType);
            return;
        }
        db.getCollection(aggregateType).renameCollection(new MongoNamespace(db.getName(), aggregateType + "_old"));
        createIndexes(db, aggregateType);
        long count = 0;
        boolean readall = false;
        while(!readall) {
            final ArrayList<Event> events = new ArrayList<>();
            final ArrayList<Messages.EventWrapper> eventWrappers = new ArrayList<>();
            readall = loadEventsAndHandle(aggregateType + "_old", event -> {
                log.info("upgrading event {}", event);
                        Messages.EventWrapper eventWrapper = event.upgradeToProto();
                if(eventWrapper != null && eventWrapper.getVersion() != -1) {
                    if("BRUKER".equalsIgnoreCase(aggregateType)){
                        //version is broken in bruker aggregate
                        eventWrapper = eventWrapper.toBuilder().setVersion(-1).build();
                    }
                    events.add(event);
                    eventWrappers.add(eventWrapper);
                } else {
                    log.error("failed to upgrade event {}", event);
                }
            }, String.valueOf(count));
            if(events.size() > 0)
                count = Long.parseLong(events.get(events.size()-1).getJournalid());
            else
                return;
            saveEventsBatch(eventWrappers);
            log.info("Upgraded events to journalid {} for aggregateType {}", count, aggregateType);
        }

    }

    class Counter {

        int i = 0;

        public void increment(){
            i++;
        }

        public int getValue(){
            return i;
        }

    }
    public boolean loadEventsAndHandle(final String aggregateType, HandleEventMetadata handleEvent, long fromKey, int readlimit) {
        final Document query = new Document("jid", new Document("$gt", fromKey));
        FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> db.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(readlimit));
        final Counter counter = new Counter();
        dbObjects.forEach((Consumer<Document>) document -> {
            try {
                Messages.EventWrapper event = deSerialize(document, aggregateType);
                handleEvent.handleEvent(event);
            } catch (Exception e) {
                log.error("Failed to read serialized class" + document.toString(), e);
                throw e;
            }
            counter.increment();
        });
        return counter.getValue() < readlimit;
    }

    boolean loadEventsAndHandle(final String aggregateType, HandleEvent handleEvent, String fromKey, final int readlimit) {
        final Document query = new Document("jid", new Document("$gt", Long.parseLong(fromKey)));
        FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> db.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(readlimit));
        final Counter counter = new Counter();
        dbObjects.forEach((Consumer<Document>) document -> {
            try {
                final Event event = deSerialize(((Binary) document.get("d")).getData());
                if (!("" + document.get("jid")).equals(event.getJournalid())) {
                    log.error("Journalid in database dosen't match event db: {} event: {} : completeevent:{}", document.get("jid"), event.getJournalid(), event);
                }
                Event upgradedEvent = event.upgrade();
                while(!upgradedEvent.equals(upgradedEvent.upgrade())){
                    upgradedEvent = upgradedEvent.upgrade();
                }


                handleEvent.handleEvent(upgradedEvent);
            } catch (Exception e) {
                log.error("Failed to read serialized class" + document.toString(), e);
                throw e;
            }
            counter.increment();
        });

        return counter.getValue() < readlimit;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage) {
       throw new RuntimeException("Upgrade not implemented");
    }

    @Override
    public void doBackup(String backupDirectory, String backupfilename) {

    }

    @Override
    public EventBatch loadEventsForAggregateId(final String aggregateType, String aggregateId, String fromJournalId) {
        final Document query = new Document("rid", aggregateId);
        if (fromJournalId != null)
            query.append("jid", new Document("$gt", Long.parseLong(fromJournalId)));

        final FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> db.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(eventReadLimit));

        final List<Event> events = StreamSupport.stream(dbObjects.spliterator(), false)
                .map(document -> deSerialize( ((Binary)document.get("d")).getData()))
                .collect(Collectors.toList());

        return new EventBatch(aggregateType, aggregateId, events, events.size() != eventReadLimit);
    }

    @Override
    public Future<EventBatch> loadEventsForAggregateIdAsync(final String aggregateType, final String aggregateId, final String fromJournalId) {
        final Document query = new Document("rid", aggregateId);
        if (fromJournalId != null)
            query.append("jid", new Document("$gt", Long.parseLong(fromJournalId)));

        final ArrayList<Event> events = new ArrayList<>();
        com.mongodb.async.client.FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> dbasync.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(eventReadLimit));

        final Promise<EventBatch> promise = Futures.promise();
        final Future<EventBatch> theFuture = promise.future();
        dbObjects.forEach(document -> events.add(deSerialize(((Binary) document.get("d")).getData())), (result, t) -> promise.success(new EventBatch(aggregateType, aggregateId, events, events.size() != eventReadLimit)));
        return theFuture;
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(final String aggregateType, final String aggregateRootId, final long fromJournalId) {
        final Document query = new Document("rid", aggregateRootId);
        query.append("jid", new Document("$gt", fromJournalId));

        final com.mongodb.async.client.FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> dbasync.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(eventReadLimit));

        final Promise<Messages.EventWrapperBatch> promise = Futures.promise();

        dbObjects.map(document -> deSerialize(document,aggregateType))
                .into(new ArrayList<>(), (SingleResultCallback<ArrayList>) (list, throwable) -> promise.success(Messages.EventWrapperBatch.newBuilder()
                .addAllEvents(list)
                .setAggregateType(aggregateType)
                .setReadAllEvents(list.size() != eventReadLimit)
                .setAggregateRootId(aggregateRootId)
                .build()));

        return promise.future();
    }

    @Override
    public Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(final String aggregateType, final String correlationId, final long fromJournalId) {
        final Document query = new Document("correlationid", correlationId);
        query.append("jid", new Document("$gt", fromJournalId));

        final com.mongodb.async.client.FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> dbasync.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(eventReadLimit));

        final Promise<Messages.EventWrapperBatch> promise = Futures.promise();

        dbObjects.map(document -> deSerialize(document,aggregateType))
                .into(new ArrayList<>(), (SingleResultCallback<ArrayList>) (list, throwable) -> promise.success(Messages.EventWrapperBatch.newBuilder()
                        .addAllEvents(list)
                        .setAggregateType(aggregateType)
                        .setReadAllEvents(list.size() != eventReadLimit)
                        .build()));

        return promise.future();
    }

    @Override
    public Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId) {
        final Document query = new Document("rid", aggregateRootId);
        query.append("jid", new Document("$gt", fromJournalId));

        final FindIterable<Document> dbObjects = MongoDbOperations.doDbOperation(() -> db.getCollection(aggregateType).find(query).sort(new Document("jid", 1)).limit(eventReadLimit));

        final List<Messages.EventWrapper> events = StreamSupport.stream(dbObjects.spliterator(), false)
                .map(document -> deSerialize( document, aggregateType))
                .collect(Collectors.toList());

        final Messages.EventWrapperBatch build = Messages.EventWrapperBatch.newBuilder()
                .setAggregateRootId(aggregateRootId)
                .setAggregateType(aggregateType)
                .setReadAllEvents(events.size() != eventReadLimit)
                .addAllEvents(events).build();
        return build;
    }
}
