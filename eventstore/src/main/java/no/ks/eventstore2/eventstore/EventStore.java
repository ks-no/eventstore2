package no.ks.eventstore2.eventstore;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import com.google.common.collect.Lists;
import eventstore.Messages;
import no.ks.eventstore2.TakeSnapshot;
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
                .match(Messages.RetreiveAggregateEventsAsync.class, this::readAggregateEvents)
                .match(Messages.RetreiveAggregateEvents.class, this::readAggregateEvents)
                .match(Messages.RetreiveCorrelationIdEventsAsync.class, this::readAggregateEvents)
                .match(Messages.AcknowledgePreviousEventsProcessed.class, o -> sender().tell(Messages.Success.getDefaultInstance(), self()))
                .match(TakeSnapshot.class, this::handleTakeSnapshot)
                .build();
    }

    private void handleFail(Object o) {
//        eventstoresingeltonProxy.tell(o, sender());
        throw new RuntimeException("Failing by force"); // TODO: What to do? singleton kastet exception
    }

    private void handleTakeSnapshot(TakeSnapshot o) { // TODO: Sende til alle projeksjoner via ProjectionManager
        throw new RuntimeException("Implement this!");
//                for (ActorRef actorRef : aggregateSubscribers.values()) {
//                    actorRef.tell(o, self());
//                }
    }

    private void readAggregateEvents(Messages.RetreiveAggregateEventsAsync retreiveAggregateEvents) {
        final ActorRef sender = sender();
        final ActorRef self = self();
        final Future<Messages.EventWrapperBatch> future =
                storage.loadEventWrappersForAggregateIdAsync(
                        retreiveAggregateEvents.getAggregateType(),
                        retreiveAggregateEvents.getAggregateRootId(),
                        retreiveAggregateEvents.getFromJournalId());
        future.onSuccess(new OnSuccess<Messages.EventWrapperBatch>() {
            @Override
            public void onSuccess(Messages.EventWrapperBatch result) throws Throwable {
                sender.tell(result, self);
            }
        }, getContext().dispatcher());
        future.onFailure(new OnFailure() {
                             @Override
                             public void onFailure(Throwable failure) throws Throwable {
                                 log.error("failed to read events from journalstorage {} ", retreiveAggregateEvents, failure);
                             }
                         }, getContext().dispatcher()
        );
    }

    private void readAggregateEvents(Messages.RetreiveCorrelationIdEventsAsync retreiveAggregateEvents) {
        final ActorRef sender = sender();
        final ActorRef self = self();
        final Future<Messages.EventWrapperBatch> future = storage.loadEventWrappersForCorrelationIdAsync(retreiveAggregateEvents.getAggregateType(), retreiveAggregateEvents.getCorrelationId(), retreiveAggregateEvents.getFromJournalId());
        future.onSuccess(new OnSuccess<Messages.EventWrapperBatch>() {
            @Override
            public void onSuccess(Messages.EventWrapperBatch result) throws Throwable {
                sender.tell(result, self);
            }
        }, getContext().dispatcher());
        future.onFailure(new OnFailure() {
                             @Override
                             public void onFailure(Throwable failure) throws Throwable {
                                 log.error("failed to read events from journalstorage {} ", retreiveAggregateEvents, failure);
                             }
                         }, getContext().dispatcher()
        );
    }

    private void storeEventWrapper(Messages.EventWrapper o) {
        storage.saveEvent(o);
    }

    private void storeEventWrapperBatch(Messages.EventWrapperBatch o) {
        Lists.partition(o.getEventsList(), 500).forEach(subBatch -> storage.saveEventsBatch(subBatch));
    }

    private void readAggregateEvents(Messages.RetreiveAggregateEvents retreiveAggregateEvents) {
        sender().tell(
                storage.loadEventWrappersForAggregateId(
                        retreiveAggregateEvents.getAggregateType(),
                        retreiveAggregateEvents.getAggregateRootId(),
                        retreiveAggregateEvents.getFromJournalId()),
                self());
    }
}
