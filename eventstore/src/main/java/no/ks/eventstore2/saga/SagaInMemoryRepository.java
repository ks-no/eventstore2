package no.ks.eventstore2.saga;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SagaInMemoryRepository extends SagaRepository{

	private Map<String, Byte> map = new HashMap<>();

    @Override
    public void saveState(String sagaStateId, String sagaid, byte state) {
        map.put(sagaStateId + "_" +  sagaid, state);
    }

    @Override
    public byte getState(String sagaStateId, String sagaid) {
        return  (map.containsKey(sagaStateId + "_" +  sagaid) ? map.get(sagaStateId + "_" +  sagaid) : Saga.STATE_INITIAL);
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
        return 0;
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
