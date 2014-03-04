package no.ks.eventstore2.saga;

import com.mongodb.*;

import java.util.List;

public class SagaMongoDBRepository extends SagaRepository{
    private final DBCollection states;
    private final DBCollection journalid;
    private DB db;

    public SagaMongoDBRepository(DB db) {
        this.db = db;
        states = db.getCollection("states");
        states.ensureIndex(new BasicDBObject("clz",1).append("sid",1));
        states.setWriteConcern(WriteConcern.JOURNAL_SAFE);
        journalid = db.getCollection("journalid");
        journalid.setWriteConcern(WriteConcern.JOURNAL_SAFE);
    }

    @Override
    public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
        states.update(new BasicDBObject("clz", clz.getName()).append("sid", sagaid), new BasicDBObject("clz", clz.getName()).append("sid", sagaid).append("s", state), true, false);
    }

    @Override
    public byte getState(Class<? extends Saga> clz, String sagaid) {
        DBCursor cursor = states.find(new BasicDBObject("clz", clz.getName()).append("sid", sagaid)).limit(1);
        if(!cursor.hasNext()) return 0;

        Object s = cursor.next().get("s");
        if(s instanceof Integer)
            return ((Integer) s).byteValue();
        else
            return (Byte) s;
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
    public String loadLatestJournalID(String aggregate) {
        DBCursor limit = journalid.find(new BasicDBObject("_id", aggregate)).limit(1);
        if(!limit.hasNext()) return null;
        return (String) limit.next().get("latestJournalID");
    }

    @Override
    public void saveLatestJournalId(String aggregate, String latestJournalId) {
        journalid.save(new BasicDBObject("_id", aggregate).append("latestJournalID", latestJournalId));
    }

    @Override
    public void saveStates(List<State> list) {
        states.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
        for (State state : list) {
            saveState(state.getClazz(), state.getId(), state.getState());
        }
        states.setWriteConcern(WriteConcern.JOURNAL_SAFE);

    }
}
