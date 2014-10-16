package no.ks.eventstore2.eventstore;

import no.ks.eventstore2.Event;

import java.util.List;

public interface JournalStorage {

    void saveEvent(Event event);

    /**
     * Load events
     * @param aggregateType
     * @param handleEvent
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateType, final HandleEvent handleEvent);

    /**
     * LoadEvent from a key
     * @param aggregateType
     * @param handleEvent
     * @param fromKey from this key
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateType, final HandleEvent handleEvent, String fromKey);

    void open();
    void close();

    void upgradeFromOldStorage(String aggregateType, JournalStorage oldStorage);

    /**
     *
     * @param backupDirectory
     * @param backupfilename without file ending
     */
    void doBackup(String backupDirectory, String backupfilename);

    /**
     *
     * @param aggregateType
     * @param aggregateId
     * @param fromJournalId null if read from begining
     * @return
     */
    EventBatch loadEventsForAggregateId(String aggregateType, String aggregateId, String fromJournalId);

    void saveEvents(List<? extends Event> events);
}
