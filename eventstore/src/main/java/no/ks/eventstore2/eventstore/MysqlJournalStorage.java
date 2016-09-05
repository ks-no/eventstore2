package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;
import scala.concurrent.Future;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MysqlJournalStorage extends AbstractJournalStorage {
    private static Logger log = LoggerFactory.getLogger(MysqlJournalStorage.class);

    public MysqlJournalStorage(DataSource dataSource, KryoClassRegistration kryoClassRegistration) {
        super(dataSource, kryoClassRegistration);
    }

    @Override
    public void saveEvents(List<? extends Event> events) {
        throw new UnsupportedOperationException("Please implement save events on MysqlJournalStorage");
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

    @Override
    public boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent) {
        throw new RuntimeException("NotImplemented");
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

    @Override
    public Future<EventBatch> loadEventsForAggregateIdAsync(String aggregateType, String aggregateId, String fromJournalId) {
        throw new RuntimeException("Not implemented");
    }
}