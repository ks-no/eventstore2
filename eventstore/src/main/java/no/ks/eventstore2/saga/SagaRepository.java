package no.ks.eventstore2.saga;

import java.util.List;

public abstract class SagaRepository {

	public abstract void saveState(String sagaStateId, String sagaid, byte state);

    public abstract byte getState(String sagaStateId, String sagaid);

    public abstract void close();

    public abstract void open();
    public abstract void readAllStatesToNewRepository(final SagaRepository repository);

    public abstract void doBackup(String backupdir, String backupfilename);

    public abstract String loadLatestJournalID(String aggregate);

    public abstract void saveLatestJournalId(String aggregate, String latestJournalId);

    public abstract void saveStates(List<State> list);
}