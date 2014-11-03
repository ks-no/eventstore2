package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import no.ks.eventstore2.Event;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlJournalStorage extends AbstractJournalStorage {

    private JdbcTemplate template;

	public MysqlJournalStorage(DataSource dataSource) {
	    super();
        template = new JdbcTemplate(dataSource);
    }

	@Override
	public void saveEvent(final Event event) {
		final ByteArrayOutputStream output = createByteArrayOutputStream(event);
		LobHandler lobHandler = new DefaultLobHandler();
		template.execute("INSERT INTO event (aggregatetype, class, dataversion, kryoeventdata) VALUES(?,?,?,?)",
				new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
			protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
				ps.setString(1, event.getAggregateType());
				ps.setString(2, event.getClass().getName());
				ps.setInt(3, 2);
				lobCreator.setBlobAsBytes(ps, 4, output.toByteArray());
			}
		});
	}

	public boolean loadEventsAndHandle(String aggregate, final HandleEvent handleEvent) {
		template.query("SELECT * FROM event WHERE aggregatetype = ? ORDER BY ID", new Object[]{aggregate}, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet resultSet) throws SQLException {
				if (resultSet.getInt("dataversion") == 2) {
					Blob blob = resultSet.getBlob("kryoeventdata");
					Input input = new Input(blob.getBinaryStream());
					Event event = (Event) kryo.readClassAndObject(input);
					input.close();
					event.setJournalid(resultSet.getBigDecimal("id").toPlainString());
					handleEvent.handleEvent(event);
				}
			}
		});
		return true;
	}

	@Override
	public boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey) {
		return false;
	}

	@Override
	public void open() {

	}

	@Override
	public void close() {

	}

	@Override
	public void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage) {
		throw new RuntimeException("NotImplemented");
	}

	@Override
	public void doBackup(String backupDirectory, String backupfilename) {

	}

    @Override
    public EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId) {
        return null;
    }

    protected ByteArrayOutputStream createByteArrayOutputStream(final Event event) {
		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		Output kryodata = new Output(output);
		kryo.writeClassAndObject(kryodata, event);
		kryodata.close();
		return output;
	}
}
