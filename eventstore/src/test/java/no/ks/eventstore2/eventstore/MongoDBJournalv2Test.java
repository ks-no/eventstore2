package no.ks.eventstore2.eventstore;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.esotericsoftware.kryo.Kryo;
import com.mongodb.*;
import no.ks.eventstore2.Event;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static akka.pattern.Patterns.ask;

public class MongoDBJournalv2Test extends MongoDbEventstore2TestKit {


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
        DB db = mongoClient.getDB("Journal");
        journal = new MongoDBJournalV2(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg3", "agg2"}), 10);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

    }

    @Test
    public void testSaveAndRetrieveEvent() throws Exception {
        journal.saveEvent(new AggEvent(UUID.randomUUID().toString(), "agg1"));
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        });
        assertEquals(1, events.size());
        assertEquals("agg1", events.get(0).getAggregateType());
    }

    @Test
    public void testTwoEventsWithoutVersion() throws Exception {
        final ArrayList<Event> events = new ArrayList<>();
        final AggEvent agg1 = new AggEvent("1", "agg1");
        agg1.setVersion(-1);
        final AggEvent agg2 = new AggEvent("1", "agg1");
        agg2.setVersion(-1);
        events.add(agg1);
        events.add(agg2);
        journal.saveEvents(events);
        events.clear();
        final AggEvent agg3 = new AggEvent("1", "agg1");
        agg3.setVersion(-1);
        final AggEvent agg4 = new AggEvent("1", "agg1");
        agg4.setVersion(-1);
        events.add(agg3);
        events.add(agg4);
        journal.saveEvents(events);
    }

    @Test
    public void testSaveAndReceiveEventsFromKey() throws Exception {
        journal.saveEvent(new AggEvent("agg3"));
        journal.saveEvent(new AggEvent("agg3"));
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg3", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, "1");
        assertEquals(1, events.size());
        assertEquals("agg3", events.get(0).getAggregateType());
    }

    @Test
    public void testEventReadLimit() throws Exception {
        for (int i = 0; i < 15; i++) {
            journal.saveEvent(new AggEvent("agg2"));
        }
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, "0");
        assertEquals(10, events.size());
        journal.loadEventsAndHandle("agg2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, events.get(events.size() - 1).getJournalid());
        assertEquals(15, events.size());
    }

    @Test
    public void testPartialRead() throws Exception {
        String aggregateType = "agg2";
        for (int i = 0; i < 11; i++) {
            journal.saveEvent(new AggEvent(aggregateType));
        }
        final ArrayList<Event> results = new ArrayList<Event>();
        assertFalse(journal.loadEventsAndHandle(aggregateType, new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        }));
        assertEquals(10, results.size());

        assertTrue(journal.loadEventsAndHandle(aggregateType, new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        }, "10"));
        assertEquals(11, results.size());
        assertEquals("10", results.get(9).getJournalid());
        assertEquals("11", results.get(10).getJournalid());

    }


    @Test
    public void testWritingSameVersionShouldFail() throws Exception {
        AggEvent versionFail = new AggEvent("version_failed_agg_id", "agg1");
        versionFail.setVersion(0);
        journal.saveEvent(versionFail);
        try {
            journal.saveEvent(versionFail);
            fail("Should have gotten exception");
        } catch (Exception e) {

        }

    }

    private ArrayList<Event> getEvents(MongoDBJournal journal2, String aggregateType) {
        final ArrayList<Event> events = new ArrayList<Event>();
        journal2.loadEventsAndHandle(aggregateType, new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        });
        return events;
    }

    public static final int NUMBER_OF_VERSIONS = 50;
    public static final int NUMBER_OF_AGGREGATES = 50;

    @Test
    public void testStresstest() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        final ArrayList<Future<String>> futures = new ArrayList<Future<String>>();
        for (int p = 0; p < NUMBER_OF_AGGREGATES; p++) {
            futures.add(executorService.submit(new Callable<String>() {
                @Override
                public String call() {
                    String aggregateRootId = UUID.randomUUID().toString();
                    for (int i = 0; i < NUMBER_OF_VERSIONS; i++) {
                        AggEvent agg1 = new AggEvent(aggregateRootId, "agg1");
                        agg1.setVersion(i);
                        journal.saveEvent(agg1);
                    }
                    return aggregateRootId;

                }
            }));

        }
        final ArrayList<String> aggregateIds = new ArrayList<String>();
        for (Future<String> future : futures) {
            aggregateIds.add(future.get(60, TimeUnit.SECONDS));
        }
        final ArrayList<Event> events = new ArrayList<Event>();

        final HandleEvent loadEvents = new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
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

    @Test
    public void testUpgradeFromV1() throws Exception {
        MongoDBJournal journalv1 = new MongoDBJournal(mongoClient.getDB("v1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg3", "agg2"}), 10);
        journalv1.saveEvent(new AggEvent("1_id1", "agg1"));
        journalv1.saveEvent(new AggEvent("1_id2", "agg1"));
        journalv1.saveEvent(new AggEvent("1_id1", "agg1"));
        journalv1.saveEvent(new AggEvent("2_id3", "agg2"));
        journalv1.saveEvent(new AggEvent("2_id1", "agg2"));
        journalv1.saveEvent(new AggEvent("2_id2", "agg2"));

        journal.upgradeFromOldStorage("agg1", journalv1);
        journal.upgradeFromOldStorage("agg2", journalv1);

        EventBatch eventBatch = journal.loadEventsForAggregateId("agg2", "2_id1", null);
        assertEquals(1, eventBatch.getEvents().size());
        final Event event = eventBatch.getEvents().get(0);
        assertEquals("2_id1", event.getAggregateRootId());
        assertEquals("agg2", event.getAggregateType());
        assertEquals(0,event.getVersion());

        eventBatch = journal.loadEventsForAggregateId("agg1", "1_id1", null);
        assertEquals(2, eventBatch.getEvents().size());
        assertEquals(1, eventBatch.getEvents().get(1).getVersion());
    }

    @Test
    public void testUpgrade() throws Exception {
        final MongoDBJournal journalv1 = new MongoDBJournal(mongoClient.getDB("Journalv1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg3", "agg2"}), 10);

        for(int i = 0; i<100; i++){
            journalv1.saveEvent(new AggEvent("" + i, "agg1"));
        }
        journal.upgradeFromOldStorage("agg1", journalv1);

        for(int i = 100; i<200; i++){
            journal.saveEvent(new AggEvent("" + i, "agg1"));
        }
        assertEquals(1,journalv1.loadEventsForAggregateId("agg1", "99", "0").getEvents().size());
        assertEquals(0,journalv1.loadEventsForAggregateId("agg1", "100", "0").getEvents().size());
        assertEquals(1,journal.loadEventsForAggregateId("agg1", "100", "0").getEvents().size());


    }

    @Test
    public void testUpgradeAsyncWithEventStore() throws Exception {
        final MongoDBJournal journalv1 = new MongoDBJournal(mongoClient.getDB("Journalv1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1", "agg3", "agg2"}), 10);

        ActorRef eventStore = _system.actorOf(EventStore.mkProps(journal));
        for(int i = 0; i<20; i++){
            eventStore.tell(new AggEvent("" + i, "agg1"), super.testActor());
        }
        Await.result(ask(eventStore, new AcknowledgePreviousEventsProcessed(), 3000), Duration.create("3 seconds"));


        eventStore = _system.actorOf(EventStore.mkProps(journal));
        eventStore.tell(new UpgradeAggregate(journalv1,"agg1"), super.testActor());

        for(int i = 20; i<40; i++){
            eventStore.tell(new AggEvent("" + i, "agg1"), super.testActor());
        }
        Await.result(ask(eventStore, new AcknowledgePreviousEventsProcessed(), 3000), Duration.create("3 seconds"));

        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                System.out.println(event);
            }
        }, "-1", 1000);


        for (int i = 0; i < 40; i++) {
            assertEvent("" + i);
        }

    }

    private void assertEvent(String aggregateId) {
        final List<Event> events = journal.loadEventsForAggregateId("agg1", aggregateId, "0").getEvents();
        try {
            assertEquals(1, events.size());
            assertEquals("" + (Integer.parseInt(aggregateId) +1), events.get(0).getJournalid());
        } catch (AssertionError e){
            System.out.println("failed on " + aggregateId);
            throw e;
        }

    }
}
