package no.ks.eventstore2.eventstore;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import eventstore.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

public class EventStore extends AbstractActor {

    private static Logger log = LoggerFactory.getLogger(EventStore.class);

    private JournalStorage storage;

    public static Props mkProps(JournalStorage journalStorage) {
        return Props.create(EventStore.class, journalStorage);
    }

    public EventStore(JournalStorage journalStorage) {
        storage = journalStorage;
    }

    @Override
    public void preStart() {
        log.debug("EventStore preStart");
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        try {
            super.aroundReceive(receive, msg);
        } catch (Exception e) {
            log.error("Eventstore got an error: ", e);
            throw e;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("fail", this::handleFail)
                .match(Messages.EventWrapper.class, this::storeEventWrapper)
                .match(Messages.EventWrapperBatch.class, this::storeEventWrapperBatch)
                .match(Messages.RetreiveAggregateEvents.class, this::readAggregateEvents)
                .match(Messages.RetreiveAggregateEventsAsync.class, this::readAggregateEventsAsync)
                .match(Messages.RetreiveCorrelationIdEventsAsync.class, this::readCorrelationIdEventsAsync)
                .match(Messages.AcknowledgePreviousEventsProcessed.class, o -> sender().tell(Messages.Success.getDefaultInstance(), self()))
                .build();
    }

    private void handleFail(Object o) {
        throw new RuntimeException("Failing by force");
    }

    private void storeEventWrapper(Messages.EventWrapper o) {
        storage.saveEvent(o);
    }

    private void storeEventWrapperBatch(Messages.EventWrapperBatch o) {
        Lists.partition(o.getEventsList(), 500).forEach(subBatch -> storage.saveEventsBatch(subBatch));
    }

    private void readAggregateEvents(Messages.RetreiveAggregateEvents retreiveAggregateEvents) throws Exception {
        sender().tell(
                storage.loadEventWrappersForAggregateId(
                        retreiveAggregateEvents.getAggregateType(),
                        retreiveAggregateEvents.getAggregateRootId(),
                        retreiveAggregateEvents.getFromJournalId()),
                self());
    }

    private void readAggregateEventsAsync(Messages.RetreiveAggregateEventsAsync retreiveAggregateEventsAsync) {
        final Future<Messages.EventWrapperBatch> future =
                storage.loadEventWrappersForAggregateIdAsync(
                        retreiveAggregateEventsAsync.getAggregateType(),
                        retreiveAggregateEventsAsync.getAggregateRootId(),
                        retreiveAggregateEventsAsync.getFromJournalId());
        setupEventWrapperBatchFutureHandling(future, retreiveAggregateEventsAsync, self(), sender());
    }

    private void readCorrelationIdEventsAsync(Messages.RetreiveCorrelationIdEventsAsync retreiveCorrelationIdEventsAsync) {
        final Future<Messages.EventWrapperBatch> future =
                storage.loadEventWrappersForCorrelationIdAsync(
                        retreiveCorrelationIdEventsAsync.getAggregateType(),
                        retreiveCorrelationIdEventsAsync.getCorrelationId(),
                        retreiveCorrelationIdEventsAsync.getFromJournalId());
        setupEventWrapperBatchFutureHandling(future, retreiveCorrelationIdEventsAsync, self(), sender());
    }

    private void setupEventWrapperBatchFutureHandling(Future<Messages.EventWrapperBatch> future, Message message, ActorRef self, ActorRef sender) {
        future.onSuccess(new OnSuccess<Messages.EventWrapperBatch>() {
            @Override
            public void onSuccess(Messages.EventWrapperBatch result) {
                sender.tell(result, self);
            }
        }, getContext().dispatcher());
        future.onFailure(new OnFailure() {
            @Override
            public void onFailure(Throwable failure) {
                log.error("Failed to read events from Journal Storage: {} ", message, failure);
            }
        }, getContext().dispatcher());
    }
}
