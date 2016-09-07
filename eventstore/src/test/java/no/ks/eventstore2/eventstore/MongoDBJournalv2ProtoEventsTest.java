package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.google.protobuf.Any;
import com.mongodb.client.MongoDatabase;
import events.Aggevents.Agg;
import events.test.Order.Order;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class MongoDBJournalv2ProtoEventsTest extends MongoDbEventstore2TestKit {


    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private MongoDBJournalV2 journal;


    @Before
    public void setUp() throws Exception {
        super.setUp();

        ProtobufHelper.registerDeserializeMethod(Agg.Aggevent.getDefaultInstance());
        ProtobufHelper.registerDeserializeMethod(Order.SearchRequest.getDefaultInstance());
        MongoDatabase db = mongoClient.getDatabase("Journal");
        journal = new MongoDBJournalV2(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg3", "agg2"}), 10, null);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

    }

    @Test
    public void testSaveAndRetrieveEvent() throws Exception {
        final Order.SearchRequest searchRequest = Order.SearchRequest.newBuilder().setQuery("Select all requests").setPageNumber(5).build();
        Messages.EventWrapper eventWrapper = ProtobufHelper.newEventWrapper("agg1", UUID.randomUUID().toString(), searchRequest);
        journal.saveEvent(eventWrapper);
        final List<Messages.EventWrapper> events = new ArrayList<>();
        journal.loadEventsAndHandle("agg1", new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        });
        assertEquals(1, events.size());
        assertEquals("agg1", events.get(0).getAggregateType());
        final Any event = events.get(0).getEvent();
        assertTrue(event.is(Order.SearchRequest.class));
        assertEquals(searchRequest, event.unpack(Order.SearchRequest.class)); //ProtobufHelper.deserializeAny(events.get(0).getProtoSerializationType(), events.get(0).getEvent()));
    }

    @Test
    public void testTwoEventsWithoutVersion() throws Exception {
        final ArrayList<Messages.EventWrapper> events = new ArrayList<>();
        events.add(ProtobufHelper.newEventWrapper("agg1", "1", -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()));
        events.add(ProtobufHelper.newEventWrapper("agg1", "1", -1, Order.SearchResult.newBuilder().addResult("res1").addResult("res2").build()));
        journal.saveEvents(events);
        events.clear();
        events.add(ProtobufHelper.newEventWrapper("agg1", "1", -1, Order.SearchRequest.newBuilder().setQuery("query").setPageNumber(4).build()));
        events.add(ProtobufHelper.newEventWrapper("agg1", "1", -1, Order.SearchResult.newBuilder().addResult("res1").addResult("res2").build()));
        journal.saveEvents(events);
    }

    @Test
    public void testSaveAndReceiveEventsFromKey() throws Exception {
        journal.saveEvent(ProtobufHelper.newEventWrapper("agg3", "1", 0, Order.SearchRequest.newBuilder().build()));
        journal.saveEvent(ProtobufHelper.newEventWrapper("agg3", "1", 1, Order.SearchRequest.newBuilder().build()));
        final List<Messages.EventWrapper> events = new ArrayList<>();
        journal.loadEventsAndHandle("agg3", new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        }, 1, 1000);
        assertEquals(1, events.size());
        assertEquals("agg3", events.get(0).getAggregateType());
    }

    @Test
    public void testEventReadLimit() throws Exception {
        for (int i = 0; i < 15; i++) {
            journal.saveEvent(ProtobufHelper.newEventWrapper("agg2", "1", -1, Order.SearchRequest.newBuilder().build()));
        }
        final List<Messages.EventWrapper> events = new ArrayList<>();
        journal.loadEventsAndHandle("agg2", new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        }, 0, 10);
        assertEquals(10, events.size());
        journal.loadEventsAndHandle("agg2", new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        }, events.get(events.size() - 1).getJournalid(), 10);
        assertEquals(15, events.size());
    }

    @Test
    public void testWritingSameVersionShouldFail() throws Exception {
        AggEvent versionFail = new AggEvent("version_failed_agg_id", "agg1");
        versionFail.setVersion(0);
        journal.saveEvent(ProtobufHelper.newEventWrapper("agg1", "version_failed_agg_id", 0, Order.SearchResult.newBuilder().build()));
        try {
            journal.saveEvent(versionFail);
            fail("Should have gotten exception");
        } catch (Exception e) {

        }

    }

    public static final int NUMBER_OF_VERSIONS = 50;
    public static final int NUMBER_OF_AGGREGATES = 50;

    @Test
    public void testStresstest() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final ArrayList<Future<String>> futures = new ArrayList<Future<String>>();
        for (int p = 0; p < NUMBER_OF_AGGREGATES; p++) {
            futures.add(executorService.submit(() -> {
                String aggregateRootId = UUID.randomUUID().toString();
                for (int i = 0; i < NUMBER_OF_VERSIONS; i++) {
                    journal.saveEvent(ProtobufHelper.newEventWrapper("agg1", aggregateRootId, -1, Order.SearchRequest.newBuilder().build()));
                }
                return aggregateRootId;
            }));
        }
        final ArrayList<String> aggregateIds = new ArrayList<String>();
        for (Future<String> future : futures) {
            aggregateIds.add(future.get(60, TimeUnit.SECONDS));
        }
        final ArrayList<Messages.EventWrapper> events = new ArrayList<>();

        final HandleEventMetadata loadEvents = new HandleEventMetadata() {
            @Override
            public void handleEvent(Messages.EventWrapper event) {
                events.add(event);
            }
        };
        boolean finished = journal.loadEventsAndHandle("agg1", loadEvents);
        while(!finished){
            finished = journal.loadEventsAndHandle("agg1", loadEvents, events.get(events.size()-1).getJournalid());
        }
        assertEquals(NUMBER_OF_AGGREGATES * NUMBER_OF_VERSIONS, mongoClient.getDB("Journal").getCollection("agg1").find().size());
        assertEquals(NUMBER_OF_AGGREGATES * NUMBER_OF_VERSIONS, events.size());
    }
}
