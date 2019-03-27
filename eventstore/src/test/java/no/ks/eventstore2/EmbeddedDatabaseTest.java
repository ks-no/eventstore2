package no.ks.eventstore2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

public class EmbeddedDatabaseTest {
	protected EmbeddedDatabase db;

	@BeforeEach
	public void setUp() throws Exception {
		EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
		db = builder.setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
	}

	@Test
	public void testNothing() throws Exception {

	}

	@AfterEach
	public void tearDown() throws Exception {
		db.shutdown();
	}
}
