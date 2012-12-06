package no.ks.eventstore2.saga;

import no.ks.eventstore2.EmbeddedDatabaseTest;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SagaDatasourceRepositoryTest extends EmbeddedDatabaseTest{

	@Test
	public void testSaveAndRetrieveState() throws Exception {
		SagaDatasourceRepository repo = new SagaDatasourceRepository(db);
		repo.saveState(FormProcess.class, "id", (byte) 45);
		assertEquals(45,repo.getState(FormProcess.class, "id"));
	}
}
