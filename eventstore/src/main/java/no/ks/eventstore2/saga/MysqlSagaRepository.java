package no.ks.eventstore2.saga;

import javax.sql.DataSource;

public class MysqlSagaRepository extends SqlSagaRepository {

	public MysqlSagaRepository(DataSource dataSource) {
		super(dataSource);
	}

	@Override
	protected String getUpdateSagaSql() {
		return "update saga set state = ? where id = ? and clazz = ?";
	}

	@Override
	protected String getInsertSagaSql() {
		return "insert into saga (id,clazz,state) values(?,?,?)";
	}

	@Override
	protected String getSelectStateSql() {
		return "select state from saga where id= ? and clazz = ?";
	}

}
