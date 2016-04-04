package no.ks.eventstore2.saga;

import com.mongodb.DB;
import no.ks.eventstore2.projection.MongoDbEventstore2TestKit;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SagaMongoDBRepositoryTest extends MongoDbEventstore2TestKit {

    private SagaMongoDBRepository repo;
    private DB db;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        db = mongoClient.getDB("SagaStore");
        repo = new SagaMongoDBRepository(db);
    }

    @Test
    public void testSaveAndGetState() throws Exception {
        repo.saveState("FormProcess", "id2", (byte) 45);
        assertEquals(45, repo.getState("FormProcess", "id2"));
    }

    @Test
    public void testStateUpdateGenerateOneDocument() throws Exception {
        db = mongoClient.getDB("SagaStore2");
        repo = new SagaMongoDBRepository(db);
        repo.saveState("FormProcess", "id5", (byte) 45);
        repo.saveState("FormProcess", "id5", (byte) 45);
        assertEquals(1, db.getCollection("states").count());
    }

    @Test
    public void testTwoSaves() throws Exception {
        repo.saveState("FormProcess", "b30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
        repo.saveState("FormProcess", "a30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
        repo.saveState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 47);
        assertEquals(47,repo.getState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85"));
    }

    @Test
    public void testNullValueInGetState() throws Exception {
        assertEquals((byte)0, repo.getState("FormProcess", "NotValidSagaID"));
    }

    @Test
    public void testSaveLatestJournalId() throws Exception {
        repo.saveLatestJournalId("agg","1");
        assertEquals("1", repo.loadLatestJournalID("agg"));
    }
}
