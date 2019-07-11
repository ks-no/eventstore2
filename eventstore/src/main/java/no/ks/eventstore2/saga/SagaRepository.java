package no.ks.eventstore2.saga;

import org.joda.time.DateTime;

import java.util.List;

public abstract class SagaRepository {

    public abstract void storeScheduleAwake(String sagaid, String sagaclass, DateTime when);

    public abstract void clearAwake(String sagaid, String sagaclass);

    public abstract List<SagaCompositeId> whoNeedsToWake();

    public abstract void saveState(String sagaStateId, String sagaid, byte state);

    public abstract byte getState(String sagaStateId, String sagaid);

    public abstract void close();

    public abstract void open();
    public abstract void readAllStatesToNewRepository(SagaRepository repository);

    public abstract void doBackup(String backupdir, String backupfilename);

    public abstract long loadLatestJournalID(String aggregate);

    public abstract void saveLatestJournalId(String aggregate, long latestJournalId);

    public abstract void saveStates(List<State> list);
}