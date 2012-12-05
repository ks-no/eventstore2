package sample.cluster

import org.h2.jdbcx.JdbcDataSource
import org.springframework.test.jdbc.{JdbcTestUtils, SimpleJdbcTestUtils}
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate
import reflect.io.File
import javax.sql.DataSource

object Database {

  def datasource = {
    val ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:/tmp/test");
    ds.setUser("sa");
    ds.setPassword("sa");
    ds;
  }

  def setupDatabase(ds:DataSource){
    val template = new SimpleJdbcTemplate(ds);
    val resource = new ClassPathResource("schema.sql");
    SimpleJdbcTestUtils.executeSqlScript(template, resource, true);
  }

  def cleanDatabase = {
    File("/tmp/test.h2.db").deleteIfExists();
  }
}
