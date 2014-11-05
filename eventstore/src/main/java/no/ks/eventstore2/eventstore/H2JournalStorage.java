package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.io.Input;
import no.ks.eventstore2.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class H2JournalStorage extends AbstractJournalStorage {

    private static Logger log = LoggerFactory.getLogger(H2JournalStorage.class);

    private JdbcTemplate template;

    public H2JournalStorage(DataSource dataSource, KryoClassRegistration kryoClassRegistration) {
        super(kryoClassRegistration);
        template = new JdbcTemplate(dataSource);
    }

    public void saveEvent(final Event event) {
        final ByteArrayOutputStream output = createByteArrayOutputStream(event);
        LobHandler lobHandler = new DefaultLobHandler();
        final long id = template.queryForLong("select seq.nextval from dual");
        event.setJournalid(String.valueOf(id));
        template.execute("INSERT INTO event (id, aggregatetype, class, dataversion, kryoeventdata) VALUES(?,?,?,?,?)",
                new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
                    protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
                        ps.setBigDecimal(1, new BigDecimal(id));
                        ps.setString(2, event.getAggregateType());
                        ps.setString(3, event.getClass().getName());
                        ps.setInt(4, 2);
                        lobCreator.setBlobAsBytes(ps, 5, output.toByteArray());
                    }
                }
        );
    }

    public boolean loadEventsAndHandle(String aggregateType, final HandleEvent handleEvent) {
        return loadEventsAndHandle(aggregateType, handleEvent, "0");
    }

    @Override
    public boolean loadEventsAndHandle(String aggregateType, final HandleEvent handleEvent, String fromKey) {
        template.query("SELECT * FROM event WHERE aggregatetype = ? AND id >= ? ORDER BY id", new Object[]{aggregateType, Long.parseLong(fromKey)}, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet resultSet) throws SQLException {
                if (resultSet.getInt("dataversion") == 2) {
                    Blob blob = resultSet.getBlob("kryoeventdata");
                    Input input = new Input(blob.getBinaryStream());
                    Event event = (Event) getKryo().readClassAndObject(input);
                    input.close();
                    event.setJournalid(resultSet.getBigDecimal("id").toPlainString());
                    handleEvent.handleEvent(event);
                }
            }
        });
        return true;
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

}
