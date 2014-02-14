package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

public interface JournalStorage {

    void saveEvent(Event event);

    /**
     * Load events
     * @param aggregateid
     * @param handleEvent
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateid, final HandleEvent handleEvent);

    /**
     * LoadEvent from a key
     * @param aggregateid
     * @param handleEvent
     * @param fromKey from this key
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateid, final HandleEvent handleEvent, String fromKey);

    void open();
    void close();

    void upgradeFromOldStorage(String aggregateId, JournalStorage oldStorage);

    /**
     *
     * @param backupDirectory
     * @param backupfilename without file ending
     */
    void doBackup(String backupDirectory, String backupfilename);
}
