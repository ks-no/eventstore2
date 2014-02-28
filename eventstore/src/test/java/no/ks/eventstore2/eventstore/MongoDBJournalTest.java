package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.FongoDB;
import no.ks.eventstore2.Event;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoDBJournalTest {

    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };
    private MongoDBJournal journal;

    @Before
    public void setUp() throws Exception {

        Fongo fongo = new Fongo("mongo server 1");
        DB db = fongo.getDB("Journal");
        journal = new MongoDBJournal(db, kryoClassRegistration, Arrays.asList(new String[] {"agg1"}), 10);
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
        assertEquals("agg1", events.get(0).getAggregateId());
    }

    @Test
    public void testSaveAndReceiveEventsFromKey() throws Exception {
        journal.saveEvent(new AggEvent("agg1"));
        journal.saveEvent(new AggEvent("agg1"));
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, "1");
        assertEquals(1,events.size());
        assertEquals("agg1", events.get(0).getAggregateId());
    }

    @Test
    public void testEventReadLimit() throws Exception {
        for(int i = 0; i<15;i++){
            journal.saveEvent(new AggEvent("agg1"));
        }
        final List<Event> events = new ArrayList<Event>();
        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, "0");
        assertEquals(10,events.size());
        journal.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                events.add(event);
            }
        }, events.get(events.size()-1).getJournalid());
        assertEquals(15,events.size());
    }

    @Test
    public void testUpgradeIsOK() throws Exception {
        MongoDBJournal journal1 = new MongoDBJournal(new Fongo("fongo1").getDB("test1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal1.saveEvent(new AggEvent("agg1"));

        MongoDBJournal journal2 = new MongoDBJournal(new Fongo("fongo2").getDB("test1"), kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal2.upgradeFromOldStorage("agg1", journal1);

        ArrayList<Event> events = getEvents(journal2);
        assertEquals(1, events.size());

        journal2.upgradeFromOldStorage("agg1", journal1);
        events = getEvents(journal2);
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
            journal.saveEvent(new AggEvent("id"));
        }
        MongoDBJournal journal2 = new MongoDBJournal(new Fongo("2").getDB("events"), kryoClassRegistration, Arrays.asList(new String[]{"id"}));
        journal2.upgradeFromOldStorage("id",journal);
        final ArrayList<Event> results = new ArrayList<Event>();
        while(!journal2.loadEventsAndHandle("id", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        })){};
        assertEquals(25, results.size());
    }

    private ArrayList<Event> getEvents(MongoDBJournal journal2) {
        final ArrayList<Event> events = new ArrayList<Event>();
        journal2.loadEventsAndHandle("agg1", new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
              events.add(event);
            }
        });
        return events;
    }
}
