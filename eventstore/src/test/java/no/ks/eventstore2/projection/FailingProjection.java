package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.Handler;

import java.util.LinkedHashMap;
import java.util.Map;

@Subscriber("Form")
public class FailingProjection extends Projection {

    private boolean failed = false;

    private Map<String, EventStoreForm.FormReceived> forms = new LinkedHashMap<>();

    public FailingProjection(ActorRef eventStore) {
        super(eventStore);
    }

    @Handler
    public void handleEvent(EventStoreForm.FormReceived event) {
        if (failed) {
            forms.put(event.getFormId(), event);
        } else {
            failed = true;
            throw new RuntimeException("Failing");
        }
    }

    public EventStoreForm.FormReceived getFormById(String formId) {
        return forms.get(formId);
    }
}

