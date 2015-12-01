package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class H2JournalStorage extends AbstractJournalStorage {
    private static Logger log = LoggerFactory.getLogger(H2JournalStorage.class);

    public H2JournalStorage(DataSource dataSource, KryoClassRegistration kryoClassRegistration) {
        super(dataSource, kryoClassRegistration);
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        throw new UnsupportedOperationException("Please implement save events on H2JournalStorage");
    }

    public void saveEvent(final Event event) {
        final ByteArrayOutputStream output = createByteArrayOutputStream(event);
        LobHandler lobHandler = new DefaultLobHandler();
        final long id = template.queryForObject("select seq.nextval from dual", Long.class);
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

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    @Override
    public void upgradeFromOldStorage(String aggregateType, JournalStorage storage) {
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