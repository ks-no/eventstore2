package no.ks.eventstore2.saga.timeout;

import akka.actor.ActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.saga.SagaEventIdProperty;
import no.ks.eventstore2.saga.SagaRepository;
import no.ks.eventstore2.saga.TimeOutSaga;

import java.util.concurrent.TimeUnit;

@SagaEventIdProperty(useAggregateRootId = true)
@Subscriber("FORM")
public class SagaTimerUt extends TimeOutSaga {

    public SagaTimerUt(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Handler
    public void handleEvent(EventStoreForm.FormParsed event){
        scheduleAwake(0, TimeUnit.SECONDS);
    }

    @Handler
    public void handleEvent(EventStoreForm.FormReceived event){
        scheduleAwake(120, TimeUnit.SECONDS);
    }

    @Override
    protected void awake() {
        System.out.println("awoke");
        transitionState(STATE_FINISHED);
    }

    @Override
    protected String getSagaStateId() {
        return "idsagatimerut";
    }
}