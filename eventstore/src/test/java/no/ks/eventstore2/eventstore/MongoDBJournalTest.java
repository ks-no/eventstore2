package no.ks.eventstore2.eventstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import no.ks.eventstore2.Event;

import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.mongodb.DB;

public class MongoDBJournalTest extends MongoDbEventstore2TestKit{

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private MongoDBJournal journal;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DB db = mongoClient.getDB("Journal");
        journal = new MongoDBJournal(db, kryoClassRegistration, Arrays.asList(new String[] {"agg1"}), 10);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

    }

    @Test
    public void testSaveAndRetrieveEvent() throws Exception {
        journal.saveEvent(new AggEvent("agg1"));
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        });
        assertEquals(1,events.size());
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
        assertEquals(1,events.size());
        assertEquals("agg3", events.get(0).getAggregateType());
    }

    @Test
    public void testEventReadLimit() throws Exception {
        for(int i = 0; i<15;i++){
            journal.saveEvent(new AggEvent("agg2"));
        }
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, "0");
        assertEquals(10,events.size());
        journal.loadEventsAndHandle("agg2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, events.get(events.size()-1).getJournalid());
        assertEquals(15,events.size());
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
        for(int i = 0; i< 11; i++){
            journal.saveEvent(new AggEvent("id"));
        }
        final ArrayList<Event> results = new ArrayList<Event>();
        assertFalse(journal.loadEventsAndHandle("id", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        }));
        assertEquals(10, results.size());

        assertTrue(journal.loadEventsAndHandle("id",new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        },"10"));
        assertEquals(11, results.size());
        assertEquals("10", results.get(9).getJournalid());
        assertEquals("11", results.get(10).getJournalid());

    }

    @Test
    public void testUpgrade_25_events() throws Exception {
        for(int i = 0; i< 25; i++){
            journal.saveEvent(new AggEvent("id2"));
        }
        MongoDBJournal journal2 = new MongoDBJournal(mongoClient.getDB("events"), kryoClassRegistration, Arrays.asList(new String[]{"id"}));
        journal2.upgradeFromOldStorage("id2",journal);
        final ArrayList<Event> results = new ArrayList<Event>();
        while(!journal2.loadEventsAndHandle("id2", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        })){};
        assertEquals(25, results.size());
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
}
