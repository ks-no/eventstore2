package no.test;

import akka.actor.ActorRef;
import akka.cluster.Increase;
import akka.cluster.TestCommand;
import no.ks.eventstore2.saga.EventIdBind;
import no.ks.eventstore2.saga.ListensTo;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaRepository;

@ListensTo(value = @EventIdBind(eventClass = Increase.class, idProperty = "id"), aggregates = "test")
public class IncreaseSaga extends Saga {

    public IncreaseSaga(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    public void handleEvent(Increase event){
        System.out.println("Got increase event " + event + " state is " + getState());
        if(getState() == STATE_INITIAL){
            transitionState(STATE_FINISHED);
            commandDispatcher.tell(new TestCommand());
        }
    }
}
