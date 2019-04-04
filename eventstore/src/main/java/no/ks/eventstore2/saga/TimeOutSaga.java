package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import eventstore.Messages;
import no.ks.eventstore2.ProtobufHelper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class TimeOutSaga extends Saga {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public TimeOutSaga(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Override
    public Receive createReceive() {
        return super.createReceiveBuilder()
                .matchEquals("awake", this::handleAwake)
                .build();
    }

    private void handleAwake(Object o) {
        log.debug("{} {} awake called", getSelf(), id);
        awake();
    }

    @Override
    protected void transitionState(byte state) {
        clearAwake();
        super.transitionState(state);
    }

    private void clearAwake() {
        getContext().parent().tell(Messages.ClearAwake.newBuilder().setSagaid(getSagaCompositeId()).build(), self());
        log.debug("{} {} cleared awake", getSelf(), id);
    }

    protected abstract void awake();

    protected void scheduleAwake(int time, TimeUnit timeUnit) {
        log.debug("{} {} scheduling awake in {} {}", getSelf(), id, time, timeUnit);
        DateTime now = DateTime.now();
        if (TimeUnit.SECONDS.equals(timeUnit)) {
            now = now.plusSeconds(time);
        } else if (TimeUnit.MILLISECONDS.equals(timeUnit)) {
            now = now.plusMillis(time);
        } else if (TimeUnit.MINUTES.equals(timeUnit)) {
            now = now.plusMinutes(time);
        } else if (TimeUnit.HOURS.equals(timeUnit)) {
            now = now.plusHours(time);
        } else if (TimeUnit.DAYS.equals(timeUnit)) {
            now = now.plusDays(time);
        } else {
            log.error("No valid DateTime units for " + timeUnit);
            return;
        }
        Messages.SagaCompositeId sagaid = getSagaCompositeId();
        log.debug("Sending awake for " + now.toString());
        getContext().parent().tell(Messages.ScheduleAwake.newBuilder().setAwake(ProtobufHelper.toTimestamp(now)).setSagaid(sagaid).build(), self());
    }

    private Messages.SagaCompositeId getSagaCompositeId() {
        return Messages.SagaCompositeId.newBuilder().setClazz(getClass().getName()).setId(id).build();
    }
}
