package no.ks.eventstore2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

public class EmbeddedDatabaseTest {
	protected EmbeddedDatabase db;

	@Before
	public void setUp() throws Exception {
		EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
		db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
	}

	@Test
	public void testNothing() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		db.shutdown();
	}
}
