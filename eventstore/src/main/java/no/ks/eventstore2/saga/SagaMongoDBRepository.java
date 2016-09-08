package no.ks.eventstore2.saga;

import com.mongodb.*;
import no.ks.eventstore2.eventstore.MongoDbOperations;

import java.util.List;
import java.util.concurrent.Callable;

public class SagaMongoDBRepository extends SagaRepository{
    private final DBCollection states;
    private final DBCollection journalid;

    public SagaMongoDBRepository(DB db) {
        states = db.getCollection("states");
        states.createIndex(new BasicDBObject("clz", 1).append("sid", 1));
        states.setWriteConcern(WriteConcern.JOURNAL_SAFE);
        journalid = db.getCollection("journalid");
        journalid.setWriteConcern(WriteConcern.JOURNAL_SAFE);
    }

    @Override
    public void saveState(final String sagaStateId, final String sagaid, final byte state) {
        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                states.update(new BasicDBObject("clz", sagaStateId).append("sid", sagaid), new BasicDBObject("clz", sagaStateId).append("sid", sagaid).append("s", state), true, false);
                return null;
            }
        });
    }

    @Override
    public byte getState(final String sagaStateId, final String sagaid) {

        final DBCursor cursor = MongoDbOperations.doDbOperation(new Callable<DBCursor>() {
            @Override
            public DBCursor call() throws Exception {

                return states.find(new BasicDBObject("clz", sagaStateId).append("sid", sagaid)).limit(1);
            }
        });
        if(MongoDbOperations.doDbOperation(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return !cursor.hasNext();
            }
        })){
        	return 0;
        }

        Object s = MongoDbOperations.doDbOperation(new Callable<DBObject>() {
            @Override
            public DBObject call() throws Exception {
                return cursor.next();
            }
        }).get("s");
        if(s instanceof Integer) {
        	return ((Integer) s).byteValue();
        } else {
        	return (Byte) s;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void open() {

    }

    @Override
    public void readAllStatesToNewRepository(SagaRepository repository) {

    }

    @Override
    public void doBackup(String backupdir, String backupfilename) {

    }

    @Override
    public long loadLatestJournalID(final String aggregate) {
        final DBCursor limit = MongoDbOperations.doDbOperation(new Callable<DBCursor>() {
            @Override
            public DBCursor call() throws Exception {
                return journalid.find(new BasicDBObject("_id", aggregate)).limit(1);
            }
        });
        if(!MongoDbOperations.doDbOperation(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return limit.hasNext();
            }
        })) {
        	return 0;
        }
        return (long) MongoDbOperations.doDbOperation(new Callable<DBObject>() {
            @Override
            public DBObject call() throws Exception {
                return limit.next();
            }
        }).get("latestJournalID");
    }

    @Override
    public void saveLatestJournalId(final String aggregate, final long latestJournalId) {
        MongoDbOperations.doDbOperation(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                journalid.save(new BasicDBObject("_id", aggregate).append("latestJournalID", latestJournalId));
                return null;
            }
        });
    }

    @Override
    public void saveStates(List<State> list) {
        states.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
        for (State state : list) {
            saveState(state.getSagaStateId(), state.getId(), state.getState());
        }
        states.setWriteConcern(WriteConcern.JOURNAL_SAFE);

    }
}
