package no.ks.eventstore2.saga;

import com.mongodb.*;

public class SagaMongoDBRepository extends SagaRepository{
    private final DBCollection states;
    private final DBCollection journalid;
    private DB db;

    public SagaMongoDBRepository(DB db) {
        this.db = db;
        states = db.getCollection("states");
        states.ensureIndex(new BasicDBObject("clz",1).append("sid",1));
        journalid = db.getCollection("journalid");
    }

    @Override
    public void saveState(Class<? extends Saga> clz, String sagaid, byte state) {
        states.insert(new BasicDBObject("clz", clz.getName()).append("sid", sagaid).append("s", state));
    }

    @Override
    public byte getState(Class<? extends Saga> clz, String sagaid) {
        DBCursor cursor = states.find(new BasicDBObject("clz", clz.getName()).append("sid", sagaid)).limit(1);
        if(!cursor.hasNext()) return 0;
        return (Byte) cursor.next().get("s");
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
}
