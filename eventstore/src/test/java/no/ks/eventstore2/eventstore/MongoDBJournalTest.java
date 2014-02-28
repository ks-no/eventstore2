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
        DB db = new Fongo("fongo1").getDB("test1");
        MongoDBJournal journal1 = new MongoDBJournal(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal1.saveEvent(new AggEvent("agg1"));
        db = new Fongo("fongo2").getDB("test1");
        MongoDBJournal journal2 = new MongoDBJournal(db, kryoClassRegistration, Arrays.asList(new String[]{"agg1"}));
        journal2.upgradeFromOldStorage("agg1", journal1);

        ArrayList<Event> events = getEvents(journal2);
        assertEquals(1, events.size());

        journal2.upgradeFromOldStorage("agg1", journal1);
        events = getEvents(journal2);
        assertEquals(1, events.size());
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
