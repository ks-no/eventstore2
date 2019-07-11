package no.ks.eventstore2.eventstore;

import eventstore.Messages;
import no.ks.eventstore2.Event;
import scala.concurrent.Future;

import java.util.List;

public interface JournalStorage {

    void saveEvent(Event event);

    List<Messages.EventWrapper> saveEventsBatch(List<Messages.EventWrapper> events);

    /**
     * Load events
     * @param aggregateType
     * @param handleEvent
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent);

    boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent);

    /**
     * LoadEvent from a key
     * @param aggregateType
     * @param handleEvent
     * @param fromKey from this key
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateType, HandleEvent handleEvent, String fromKey);

    /**
     * LoadEvent from a key
     * @param aggregateType
     * @param handleEvent
     * @param fromKey from this key
     * @return true if all events sent
     */
    boolean loadEventsAndHandle(String aggregateType, HandleEventMetadata handleEvent, long fromKey);

    void open();
    void close();

    void upgradeFromOldStorage(String aggregateType, JournalStorage storage);

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

    /**
     *
     * @param aggregateType
     * @param aggregateId
     * @param fromJournalId null if read from begining
     * @return
     */
    Future<EventBatch> loadEventsForAggregateIdAsync(String aggregateType, String aggregateId, String fromJournalId);

    void saveEvents(List<? extends Event> events);

    Messages.EventWrapper saveEvent(Messages.EventWrapper eventWrapper);

    Future<Messages.EventWrapperBatch> loadEventWrappersForAggregateIdAsync(String aggregateType, String aggregateRootId, long fromJournalId);

    Future<Messages.EventWrapperBatch> loadEventWrappersForCorrelationIdAsync(String aggregateType, String correlationId, long fromJournalId);

    Messages.EventWrapperBatch loadEventWrappersForAggregateId(String aggregateType, String aggregateRootId, long fromJournalId);
}
