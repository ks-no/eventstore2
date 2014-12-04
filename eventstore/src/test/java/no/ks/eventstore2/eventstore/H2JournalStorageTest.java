package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import no.ks.eventstore2.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import static org.junit.Assert.assertTrue;

public class H2JournalStorageTest {

    private H2JournalStorage h2JournalStorage;
    private String aggregateType = "aggregateType";
    private EmbeddedDatabase dataSource;
    private Event lastEvent;

    @Before
    public void setUp() throws Exception {
        dataSource = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
        h2JournalStorage = new H2JournalStorage(dataSource, createKryoClassRegistration());
        for(int i=0; i < 2035; i++) {
            h2JournalStorage.saveEvent(new AggEvent("id_" + i, aggregateType));
        }
    }

    @After
    public void tearDown() throws Exception {
        dataSource.shutdown();
    }

    @Test
    public void testLoadingOfEvents() {
        String fromKey = "0";
        boolean finished = false;
        while(!finished) {
            if(lastEvent != null) {
                fromKey = lastEvent.getJournalid();
            }
            finished = h2JournalStorage.loadEventsAndHandle(aggregateType, createHandleEvent(), fromKey);
        }
        assertTrue(finished);
    }

    private HandleEvent createHandleEvent() {
        return new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                lastEvent = event;
            }
        };
    }

    private KryoClassRegistration createKryoClassRegistration() {
        return new KryoClassRegistration() {
            @Override
            public void registerClasses(Kryo kryo) {
                kryo.register(AggEvent.class, 1001);
            }
        };
    }
}
