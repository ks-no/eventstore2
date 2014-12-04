package no.ks.eventstore2.eventstore;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import no.ks.eventstore2.Event;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class AbstractJournalStorage implements JournalStorage {

    private final ThreadLocal<Kryo> kryoThread = new ThreadLocal<>();
    private KryoClassRegistration kryoClassRegistration;
    protected JdbcTemplate template;

    public AbstractJournalStorage(DataSource dataSource, KryoClassRegistration kryoClassRegistration) {
        this.kryoClassRegistration = kryoClassRegistration;
        template = new JdbcTemplate(dataSource);
    }

    public abstract void saveEvent(Event event);

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

    public abstract void open();

    public abstract void close();

    public abstract void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage);

    public abstract void doBackup(String backupDirectory, String backupfilename);

    public abstract EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId);

    protected ByteArrayOutputStream createByteArrayOutputStream(final Event event) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        Output kryodata = new Output(output);
        getKryo().writeClassAndObject(kryodata, event);
        kryodata.close();
        return output;
    }

    public Kryo getKryo(){
        if(kryoThread.get() == null){
            EventStoreKryo kryo = new EventStoreKryo();
            kryoClassRegistration.registerClasses(kryo);
            kryoThread.set(kryo);
        }
        return kryoThread.get();
    }
}
