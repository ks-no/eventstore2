package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import no.ks.eventstore2.Event;
import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.fusesource.leveldbjni.JniDBFactory.*;
import static org.junit.Assert.assertEquals;

public class LevelDbJournalTest {

    private LevelDbJournalStorage levelDbJournalStorage;
    private KryoClassRegistration kryoClassRegistration = new KryoClassRegistration() {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AggEvent.class, 1001);
        }
    };

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("target/journal"));

        levelDbJournalStorage = new LevelDbJournalStorage("target/journal", kryoClassRegistration);
        levelDbJournalStorage.open();
    }

    @Test
    public void testFiveEvents() throws Exception {
        levelDbJournalStorage.saveEvent(new AggEvent("id"));
        levelDbJournalStorage.saveEvent(new AggEvent("id"));
        levelDbJournalStorage.saveEvent(new AggEvent("id"));
        levelDbJournalStorage.saveEvent(new AggEvent("id"));
        levelDbJournalStorage.saveEvent(new AggEvent("id"));

        final ArrayList<Event> results = getEvents(levelDbJournalStorage, "id");
        assertEquals(5, results.size());
    }

    private ArrayList<Event> getEvents(LevelDbJournalStorage storage, String aggregateid) {
        final ArrayList<Event> results = new ArrayList<Event>();
        storage.loadEventsAndHandle(aggregateid, new HandleEvent() {
            @Override
            public void handleEvent(Event event) {
                results.add(event);
            }
        });
        return results;
    }

    @Test
    public void testGetLastKey() throws Exception {
        assertEquals(0L, levelDbJournalStorage.getNextKey("agg1"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg1"));
        assertEquals(1L, levelDbJournalStorage.getNextKey("agg1"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg1"));
        assertEquals(2L, levelDbJournalStorage.getNextKey("agg1"));
        assertEquals(0L, levelDbJournalStorage.getNextKey("agg2"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg2"));
        assertEquals(1L, levelDbJournalStorage.getNextKey("agg2"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg2"));
        assertEquals(2L, levelDbJournalStorage.getNextKey("agg2"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg1"));
        assertEquals(3L, levelDbJournalStorage.getNextKey("agg1"));
        assertEquals(0L, levelDbJournalStorage.getNextKey("agg3"));
        levelDbJournalStorage.saveEvent(new AggEvent("agg3"));
        assertEquals(1L, levelDbJournalStorage.getNextKey("agg3"));
        assertEquals(2L, levelDbJournalStorage.getNextKey("agg2"));
    }

    @Test
    public void testLevelDBTest() throws Exception {
        Options options = new Options();
        options.cacheSize(100 * 1048576); // 100MB cache
        options.createIfMissing(true);
        DB db = null;
        try {
            db = factory.open(new File("target/test"), options);
            db.put(bytes("FORSENDELSE!0000000000000000001"), bytes("value"));
            db.put(bytes("FORSENDELSE!0000000000000000002"), bytes("value"));
            db.put(bytes("berte!00001"), bytes("value"));

            DBIterator iterator = db.iterator();
            iterator.seekToFirst();
            iterator.seek(bytes("FORSENDELSE~"));
            System.out.println(iterator.hasPrev());
            System.out.println(iterator.hasNext());
            System.out.println("prev " + asString(iterator.peekPrev().getKey()));
            System.out.println("next " + asString(iterator.peekNext().getKey()));
            iterator.close();

        } finally {
            if (db != null)
                db.close();
        }

    }

    @Test
    public void testUpgradeData() throws Exception {
        FileUtils.deleteDirectory(new File("target/journal_old"));
        FileUtils.deleteDirectory(new File("target/journal_new"));
        LevelDbJournalStorage storage_new = new LevelDbJournalStorage("target/journal_new", kryoClassRegistration);
        LevelDbJournalStorage storage_OLD = new LevelDbJournalStorage("target/journal_old", kryoClassRegistration);
        try {
            storage_OLD.open();
            storage_OLD.saveEvent(new AggEvent("agg1"));
            storage_OLD.saveEvent(new AggEvent("agg1"));
            storage_OLD.saveEvent(new AggEvent("agg1"));
            storage_OLD.saveEvent(new AggEvent("agg2"));
            storage_OLD.saveEvent(new AggEvent("agg2"));
            storage_new.open();
            storage_new.upgradeFromOldStorage("agg1", storage_OLD);
            storage_new.upgradeFromOldStorage("agg2", storage_OLD);
            assertEquals(3, getEvents(storage_new, "agg1").size());
            assertEquals(2, getEvents(storage_new, "agg2").size());
        } finally {
            storage_OLD.close();
            storage_new.close();
            FileUtils.deleteDirectory(new File("target/journal_old"));
            FileUtils.deleteDirectory(new File("target/journal_new"));
        }


    }

    @After
    public void tearDown() throws Exception {
        levelDbJournalStorage.close();
        FileUtils.deleteDirectory(new File("target/journal"));
    }
}