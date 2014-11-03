package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.mongodb.*;
import no.ks.eventstore2.Event;
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
    public void testUpgradeIsOK() throws Exception {
        MongoDBJournal journal1 = new MongoDBJournal(mongoClient.getDB("test1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal1.saveEvent(new AggEvent("agg4"));

        MongoDBJournal journal2 = new MongoDBJournal(mongoClient.getDB("test2"), kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal2.upgradeFromOldStorage("agg4", journal1);

        ArrayList<Event> events = getEvents(journal2, "agg4");
        assertEquals(1, events.size());

        journal2.upgradeFromOldStorage("agg4", journal1);
        events = getEvents(journal2, "agg4");
        assertEquals(1, events.size());
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
    public void testUpgrade_25_events() throws Exception {
        for (int i = 0; i < 25; i++) {
            journal.saveEvent(new AggEvent("agg2"));
        }
        MongoDBJournal journal2 = new MongoDBJournal(mongoClient.getDB("events"), kryoClassRegistration, Arrays.asList(new String[]{"id"}));
        journal2.upgradeFromOldStorage("agg2", journal);
        final ArrayList<Event> results = new ArrayList<Event>();
        while (!journal2.loadEventsAndHandle("agg2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        })) {
        }
        ;
        assertEquals(25, results.size());
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
}
