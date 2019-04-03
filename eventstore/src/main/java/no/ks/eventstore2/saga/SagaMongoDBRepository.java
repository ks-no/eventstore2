package no.ks.eventstore2.saga;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import no.ks.eventstore2.eventstore.MongoDbOperations;
import org.bson.Document;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;

public class SagaMongoDBRepository extends SagaRepository{
    private final MongoCollection<Document> states;
    private final MongoCollection<Document> journalid;
    private final MongoCollection<Document> awake;

    public SagaMongoDBRepository(MongoDatabase db) {
        db.withWriteConcern(WriteConcern.JOURNALED);
        states = db.getCollection("states");
        states.createIndex(new BasicDBObject("clz", 1).append("sid", 1));

        awake = db.getCollection("awake");
        awake.createIndex(new Document("sid", 1).append("clz",1));
        awake.createIndex(new Document("when", -1));

        journalid = db.getCollection("journalid");
    }

    @Override
    public void storeScheduleAwake(final String sagaid, final String sagaclass, DateTime when){
        awake.updateOne(and(Filters.eq("sid", sagaid),Filters.eq("clz", sagaclass)), new Document("$set", new Document("when", when.toDate())), new UpdateOptions().upsert(true));
    }

    @Override
    public void clearAwake(final String sagaid, final String sagaclass){
        awake.deleteOne(and(Filters.eq("sid", sagaid),Filters.eq("clz", sagaclass)));
    }

    @Override
    public List<SagaCompositeId> whoNeedsToWake(){
        final ArrayList<SagaCompositeId> result = new ArrayList<>();
        awake.find(Filters.lt("when", DateTime.now().toDate())).limit(1000).map(document -> {
            try {
                return new SagaCompositeId(Class.forName(document.getString("clz")),document.getString("sid"));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException();
            }
        }).into(result);
        return result;
    }

    @Override
    public void saveState(final String sagaStateId, final String sagaid, final byte state) {
        MongoDbOperations.doDbOperation(() -> {
            states.updateOne(new Document("clz", sagaStateId).append("sid", sagaid), new Document("$set",new Document("s", state)),new UpdateOptions().upsert(true));
            return null;
        });
    }

    @Override
    public byte getState(final String sagaStateId, final String sagaid) {

        final FindIterable<Document> cursor = MongoDbOperations.doDbOperation(() -> states.find(new Document("clz", sagaStateId).append("sid", sagaid)).limit(1));

        Document d = cursor.first();
        if(d== null) return 0;

        Object s = d.get("s");
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
        final FindIterable<Document> limit = MongoDbOperations.doDbOperation(() -> journalid.find(new Document("_id", aggregate)).limit(1));
        final Document first = limit.first();
        if(first == null) return -1L;
        Object latestJournalID = first.get("latestJournalID");
        if(latestJournalID instanceof String){
            return Long.parseLong((String) latestJournalID);
        }
        if(latestJournalID == null) return -1L;
        return (long) latestJournalID;
    }

    @Override
    public void saveLatestJournalId(final String aggregate, final long latestJournalId) {
        MongoDbOperations.doDbOperation(() -> {
            journalid.updateOne(new Document("_id", aggregate), new Document("$set", new Document("latestJournalID", latestJournalId)), new UpdateOptions().upsert(true));
            return null;
        });
    }

    @Override
    public void saveStates(List<State> list) {
        MongoDbOperations.doDbOperation(() -> {
            List<WriteModel<Document>> operations = new ArrayList<>();
            for (State state : list) {
                operations.add(new UpdateOneModel<>(new Document("clz", state.getSagaStateId()).append("sid", state.getId()), new Document("$set",new Document("s", state.getState())),new UpdateOptions().upsert(true)));
            }

            final BulkWriteResult bulkWriteResult = states.bulkWrite(operations);
            return bulkWriteResult;
        });
    }
}
