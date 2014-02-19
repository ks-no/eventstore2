package no.ks.eventstore2.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SagaDatasourceRepository extends SagaRepository{

	static final Logger log = LoggerFactory.getLogger(SagaDatasourceRepository.class);
	private JdbcTemplate template;

	public SagaDatasourceRepository(DataSource dataSource) {
		template = new JdbcTemplate(dataSource);
	}

	@Override
	public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
		log.debug("Saving state {} sagaid {} state "+ state, clz, sagaid);
		int i = template.queryForInt("select count(0) from saga where id = ? and clazz = ?", sagaid, clz.getName());
		if(i > 0)
			template.update("update Saga set state = ? where id = ? and clazz = ?", state, sagaid, clz.getName());
		else
			template.update("insert into Saga (id,clazz,state) values(?,?,?)", new Object[]{sagaid, clz.getName(), state});
	}

	@Override
	public byte getState(Class<? extends Saga> clz, String sagaid) {
		int result = 0;
		try{
			 result = template.queryForInt("select state from Saga where id= ? and clazz = ?",new Object[]{sagaid, clz.getName()});
		} catch (EmptyResultDataAccessException e){

		}
		if(result > Byte.MAX_VALUE) throw new RuntimeException("Failed to convert to byte " + result);
		log.debug("Loading state from repository for clz " + clz + " sagaid " + sagaid + " state " + result);
		return (byte) result;
	}

    public void readAllStatesToNewRepository(final SagaRepository repository){
        template.query("select state,id,clazz from Saga",new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                try {
                    repository.saveState((Class<? extends Saga>)Class.forName(resultSet.getString("clazz")),resultSet.getString("id"),(byte)resultSet.getInt("state"));
                } catch (ClassNotFoundException e) {
                    log.info(e.getMessage());
                }
            }
        });
    }

    @Override
    public void doBackup(String backupdir, String backupfilename) {

    }

    @Override
    public String loadLatestJournalID(String aggregate) {
        return null;
    }

    @Override
    public void saveLatestJournalId(String aggregate, String latestJournalId) {

    }

    @Override
    public void close() {

    }

    @Override
    public void open() {

    }

}
