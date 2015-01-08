package no.ks.eventstore2.saga;

import no.ks.eventstore2.EmbeddedDatabaseTest;
import no.ks.eventstore2.formProcessorProject.FormProcess;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SagaDatasourceRepositoryTest extends EmbeddedDatabaseTest{

	@Test
	public void testSaveAndRetrieveState() throws Exception {
		SagaDatasourceRepository repo = new SagaDatasourceRepository(db);
		repo.saveState("FormProcess", "id", (byte) 45);
		assertEquals(45,repo.getState("FormProcess", "id"));
	}

	@Test
	public void testTwoSaves() throws Exception {
		SagaDatasourceRepository repo = new SagaDatasourceRepository(db);
		repo.saveState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		repo = new SagaDatasourceRepository(db);
		repo.saveState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		repo.saveState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85", (byte) 45);
		assertEquals(45,repo.getState("FormProcess", "30d91ec9-f7de-4c56-850a-1b9e4ed92e85"));



	}
}
