package no.ks.eventstore2.eventstore;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import eventstore.EventNumber;
import eventstore.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.PartialFunction;
import scala.concurrent.Future;
import scala.runtime.BoxedUnit;

import java.util.List;

public class EventStore extends AbstractActor {

    private static Logger log = LoggerFactory.getLogger(EventStore.class);

    private final ActorRef projectionManager;
    private final JournalStorage storage;

    public static Props mkProps(ActorRef projectionManager, JournalStorage journalStorage) {
        return Props.create(EventStore.class, projectionManager, journalStorage);
    }

    public EventStore(ActorRef projectionManager, JournalStorage journalStorage) {
        this.projectionManager = projectionManager;
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
                .matchEquals("fail", o -> this.handleFail())
                .matchEquals("ping", o -> this.handlePing())
                .match(Messages.EventWrapper.class, this::storeEventWrapper)
                .match(Messages.EventWrapperBatch.class, this::storeEventWrapperBatch)
                .match(Messages.RetreiveAggregateEvents.class, this::readAggregateEvents)
                .match(Messages.RetreiveAggregateEventsAsync.class, this::readAggregateEventsAsync)
                .match(Messages.RetreiveCorrelationIdEventsAsync.class, this::readCorrelationIdEventsAsync)
                .match(Messages.AcknowledgePreviousEventsProcessed.class, this::handleAcknowledgePreviousEventsProcessed)
                .build();
    }

    private void handleFail() {
        throw new RuntimeException("Failing by force");
    }

    private void handlePing() {
        sender().tell("pong", sender());
    }

    private void storeEventWrapper(Messages.EventWrapper eventWrapper) {
        Option<EventNumber.Range> writeResult = storage.saveEvent(eventWrapper);
        if (writeResult.isDefined()) {
            sender().tell(Messages.StreamPosition.newBuilder()
                    .setAggregateId(eventWrapper.getAggregateRootId())
                    .setEventNumber(writeResult.get().end().value())
                    .build(), self());
        }
    }

    private void storeEventWrapperBatch(Messages.EventWrapperBatch eventWrapperBatch) {
        Option<EventNumber.Range> lastPosition = Option.empty();
        for (List<Messages.EventWrapper> batch : Lists.partition(eventWrapperBatch.getEventsList(), 500)) {
            Option<EventNumber.Range> writeResult = storage.saveEventsBatch(batch);
            if (writeResult.isDefined()) {
                lastPosition = writeResult;
            }
        }
        if (lastPosition.isDefined()) {
            sender().tell(Messages.StreamPosition.newBuilder()
                    .setAggregateId(eventWrapperBatch.getAggregateRootId())
                    .setEventNumber(lastPosition.get().end().value())
                    .build(), self());
        }
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

    private void handleAcknowledgePreviousEventsProcessed(Messages.AcknowledgePreviousEventsProcessed message) {
        projectionManager.tell(message, sender());
    }
}
