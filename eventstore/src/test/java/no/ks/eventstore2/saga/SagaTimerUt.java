package no.ks.eventstore2.saga;

import akka.actor.ActorRef;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormReceived;
import no.ks.eventstore2.projection.Subscriber;

import java.util.concurrent.TimeUnit;

@SagaEventIdProperty(useAggregateRootId = true)
@Subscriber("FORM")
public class SagaTimerUt extends TimeOutSaga{

    public SagaTimerUt(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Handler
    public void handleEvent(FormParsed event){
        scheduleAwake(0, TimeUnit.SECONDS);
    }

    @Handler
    public void handleEvent(FormReceived event){
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
