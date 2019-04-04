package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Subscriber;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaEventIdProperty;
import no.ks.eventstore2.saga.SagaRepository;

@SagaEventIdProperty(useAggregateRootId = true)
@Subscriber("no.ks.events.svarut.Form")
public class FormProcess extends Saga {
    private static final byte FORM_RECEIVED = 2;
    private static final byte FORM_PARSED = 3;
    private static final byte FORM_DELIVERED = 4;

    public FormProcess(String id, ActorRef commandDispatcher, SagaRepository repository) {
        super(id, commandDispatcher, repository);
    }

    @Override
    protected String getSagaStateId() {
        return "FormProcess";
    }

    @Handler
    public void handleEvent(EventStoreForm.FormReceived event){
        if (getState() == STATE_INITIAL){
            commandDispatcher.tell(new ParseForm(event.getFormId()), self());
            transitionState(FORM_RECEIVED);
        }
    }

    @Handler
    public void handleEvent(EventStoreForm.FormParsed event){
        if (getState() == FORM_RECEIVED){
            commandDispatcher.tell(new DeliverForm(event.getFormId()), self());
            transitionState(FORM_PARSED);
        }
    }
    @Handler
    public void handleEvent(EventStoreForm.FormDelivered event){
        if (getState() == FORM_PARSED){
            transitionState(FORM_DELIVERED);
        }
    }
}
