package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.eventstore2.saga.EventIdBind;
import no.ks.eventstore2.saga.ListensTo;
import no.ks.eventstore2.saga.Saga;
import no.ks.eventstore2.saga.SagaRepository;

@ListensTo(value={@EventIdBind(eventClass = FormReceived.class, idProperty = "formId"), @EventIdBind(eventClass = FormParsed.class, idProperty = "formId")},aggregates = "FORM")
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

    public void handleEvent(FormReceived event){
        if (getState() == STATE_INITIAL){
            commandDispatcher.tell(new ParseForm(event.getFormId()), self());
            transitionState(FORM_RECEIVED);
        }
    }

    public void handleEvent(FormParsed event){
        if (getState() == FORM_RECEIVED){
            commandDispatcher.tell(new DeliverForm(event.getFormId()), self());
            transitionState(FORM_PARSED);
        }
    }

    public void handleEvent(FormDelivered event){
        if (getState() == FORM_PARSED){
            transitionState(FORM_DELIVERED);
        }
    }
}
