package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

    public H2JournalStorage(DataSource dataSource) {
        super();
        template = new JdbcTemplate(dataSource);
    }

    public void saveEvent(final Event event) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
        kryo.writeClassAndObject(kryodata, event);
        kryodata.close();

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

    private void updateEventToKryo(final int id, final Event event) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
        kryo.writeClassAndObject(kryodata, event);
        kryodata.close();
        log.info("Updating {} to kryo data {}", id, event);
        LobHandler lobHandler = new DefaultLobHandler();
        template.execute("update event set dataversion=2, kryoeventdata=? where id=?",
                new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
                    protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
                        lobCreator.setBlobAsBytes(ps, 1, output.toByteArray());
                        ps.setInt(2, id);
                    }
                }
        );
    }
}
