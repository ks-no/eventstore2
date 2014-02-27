package no.ks.eventstore2.saga;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SagaMongoDBRepositoryTest {

    private SagaMongoDBRepository repo;
    private DB db;

    @Before
    public void setUp() throws Exception {
        db = new Fongo("sagatest").getDB("sagastore");
        repo = new SagaMongoDBRepository(db);

    }

    @Test
    public void testSaveAndGetState() throws Exception {
        repo.saveState(FormProcess.class, "id", (byte) 45);
        assertEquals(45, repo.getState(FormProcess.class, "id"));
    }

    @Test
    public void testStateUpdateGenerateOneDocument() throws Exception {
        repo.saveState(FormProcess.class, "id", (byte) 45);
        repo.saveState(FormProcess.class, "id", (byte) 45);
        assertEquals(1, db.getCollection("states").count());
    }

    @Test
    public void testTwoSaves() throws Exception {
        repo.saveState(FormProcess.class, "b30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
        repo.saveState(FormProcess.class, "a30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
        repo.saveState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 47);
        assertEquals(47,repo.getState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85"));
    }

    @Test
    public void testNullValueInGetState() throws Exception {
        assertEquals((byte)0, repo.getState(FormProcess.class, "NotValidSagaID"));
    }

    @Test
    public void testSaveLatestJournalId() throws Exception {
        repo.saveLatestJournalId("agg","1");
        assertEquals("1", repo.loadLatestJournalID("agg"));
    }
}
