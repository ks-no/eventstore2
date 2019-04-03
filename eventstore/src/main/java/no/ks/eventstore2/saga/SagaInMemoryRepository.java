package no.ks.eventstore2.saga;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SagaInMemoryRepository extends SagaRepository{

	private Map<String, Byte> map = new HashMap<>();

	private Map<SagaCompositeId, DateTime> awakemap = new HashMap<>();

    @Override
    public void storeScheduleAwake(String sagaid, String sagaclass, DateTime when) {
        try {
            awakemap.put(new SagaCompositeId(Class.forName(sagaclass), sagaid), when);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearAwake(String sagaid, String sagaclass) {
        try {
            awakemap.remove(new SagaCompositeId(Class.forName(sagaclass), sagaid));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SagaCompositeId> whoNeedsToWake() {
        List<SagaCompositeId> result = new ArrayList<>();
        for (Map.Entry<SagaCompositeId, DateTime> sagaCompositeIdDateTimeEntry : awakemap.entrySet()) {
            if(sagaCompositeIdDateTimeEntry.getValue().isBefore(DateTime.now()))
            result.add(sagaCompositeIdDateTimeEntry.getKey());
        }
        return result;
    }

    @Override
    public void saveState(String sagaStateId, String sagaid, byte state) {
        map.put(sagaStateId + "_" +  sagaid, state);
    }

    @Override
    public byte getState(String sagaStateId, String sagaid) {
        return  map.getOrDefault(sagaStateId + "_" + sagaid, Saga.STATE_INITIAL);
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
    public long loadLatestJournalID(String aggregate) {
        return -1;
    }

    @Override
    public void saveLatestJournalId(String aggregate, long latestJournalId) {

    }

    @Override
    public void saveStates(List<State> list) {
        for (State state : list) {
            saveState(state.getSagaStateId(), state.getId(), state.getState());
        }
    }
}
