package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public abstract class TimeOutSaga extends Saga {

    private Cancellable cancellable;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public TimeOutSaga(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Override
    public void onReceive(Object o) {
        if ("awake".equals(o)) {
            log.debug("{} {} awake called", getSelf(), id);
            awake();
            return;
        }
        super.onReceive(o);
    }

    @Override
    protected void transitionState(byte state) {
        clearAwake();
        super.transitionState(state);
    }

    private void clearAwake(){
        if (cancellable != null){
            cancellable.cancel();
            log.debug("{} {} cleared awake", getSelf(), id);
        }
    }

    protected abstract void awake();

    protected void scheduleAwake(int time, TimeUnit timeUnit) {
        clearAwake();
        log.debug("{} {} scheduling awake in {} {}",getSelf(), id, time, timeUnit);
        cancellable = getContext().system().scheduler().scheduleOnce(Duration.create(time, timeUnit), self(), "awake", getContext().system().dispatcher(),self());
    }
}
