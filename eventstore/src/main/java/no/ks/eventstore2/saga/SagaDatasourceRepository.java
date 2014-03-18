package no.ks.eventstore2.saga;

import javax.sql.DataSource;

public class SagaDatasourceRepository extends SqlSagaRepository{

	public SagaDatasourceRepository(DataSource dataSource) {
		super(dataSource);
	}

	@Override
	protected String getUpdateSagaSql() {
		return "update Saga set state = ? where id = ? and clazz = ?";
	}

	@Override
	protected String getInsertSagaSql() {
		return "insert into Saga (id,clazz,state) values(?,?,?)";
	}

	@Override
	protected String getSelectStateSql() {
		return "select state from Saga where id= ? and clazz = ?";
	}
}
