package no.ks.eventstore2.saga;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

public abstract class SqlSagaRepository extends SagaRepository {

	private static final Logger log = LoggerFactory.getLogger(SagaDatasourceRepository.class);
	private JdbcTemplate template;

	public SqlSagaRepository(DataSource dataSource) {
		template = new JdbcTemplate(dataSource);
	}

	@Override
	public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
		log.debug("Saving state {} sagaid {} state "+ state, clz, sagaid);
		int i = template.queryForInt("select count(0) from saga where id = ? and clazz = ?", sagaid, clz.getName());
		if(i > 0) {
			template.update(getUpdateSagaSql(), state, sagaid, clz.getName());
		} else {
			template.update(getInsertSagaSql(), new Object[]{sagaid, clz.getName(), state});
		}
	}

	@Override
	public byte getState(Class<? extends Saga> clz, String sagaid) {
		int result = 0;
		try{
			result = template.queryForInt(getSelectStateSql(),new Object[]{sagaid, clz.getName()});
		} catch (EmptyResultDataAccessException e){

		}
		if(result > Byte.MAX_VALUE) {
			throw new RuntimeException("Failed to convert to byte " + result);
		}
		log.debug("Loading state from repository for clz " + clz + " sagaid " + sagaid + " state " + result);
		return (byte) result;
	}

	public void readAllStatesToNewRepository(final SagaRepository repository){
		final List<State> list = new ArrayList<State>();
		template.query("select state,id,clazz from Saga",new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet resultSet) throws SQLException {
				try {
					list.add(new State((Class<? extends Saga>)Class.forName(resultSet.getString("clazz")),resultSet.getString("id"),(byte)resultSet.getInt("state")));
				} catch (ClassNotFoundException e) {
					log.info(e.getMessage());
				}
			}
		});
		repository.saveStates(list);
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
	public void saveStates(List<State> list) {
		for (State state : list) {
			saveState(state.getClazz(), state.getId(), state.getState());
		}
	}

	@Override
	public void close() {

	}

	@Override
	public void open() {

	}

	protected abstract String getUpdateSagaSql();

	protected abstract String getInsertSagaSql();

	protected abstract String getSelectStateSql();

}
