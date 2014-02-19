package no.ks.eventstore2.saga;

import no.ks.eventstore2.EmbeddedDatabaseTest;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static junit.framework.Assert.assertEquals;

public class SagaLevelDBRepositoryTest extends EmbeddedDatabaseTest{

    private SagaLevelDbRepository repo;

    @Override
    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("target/sagarepo"));
        repo = new SagaLevelDbRepository("target/sagarepo");
        repo.open();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        repo.close();
        FileUtils.deleteDirectory(new File("target/sagarepo"));
    }

    @Test
	public void testSaveAndRetrieveState() throws Exception {
		repo.saveState(FormProcess.class, "id", (byte) 45);
		assertEquals(45, repo.getState(FormProcess.class, "id"));
	}

	@Test
	public void testTwoSaves() throws Exception {
		repo.saveState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		repo.close();
        repo.open();
		repo.saveState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		repo.saveState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		assertEquals(45,repo.getState(FormProcess.class, "30d91ec9-f7de-4c56-850a-1b9e4ed92e85"));
	}

    @Test
    public void testNullValueInGetState() throws Exception {
        assertEquals((byte)0, repo.getState(FormProcess.class, "NotValidSagaID"));
    }

    @Test
    public void testSaveLatestJournalId() throws Exception {
        repo.saveLatestJournalId("agg","0001");
        assertEquals("0001", repo.loadLatestJournalID("agg"));
    }
}
