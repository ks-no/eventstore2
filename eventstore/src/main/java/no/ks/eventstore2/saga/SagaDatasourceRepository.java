package no.ks.eventstore2.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

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

    @Override
    public void close() {

    }

    @Override
    public void open() {

    }

}
