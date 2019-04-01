package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.projection.Projection;
import no.ks.eventstore2.projection.Subscriber;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Subscriber("Form")
public class FormStatuses extends Projection {

    private Map<String, FormStatus> statuses = new LinkedHashMap<>();

    public FormStatuses(ActorRef eventstoreConnection) {
        super(eventstoreConnection);
    }

    public String assertEventReceived(String id) {
        if (statuses.containsKey(id)) {
            return "STATUS_RECEIVED";
        }
        return "STATUS_NOT_RECEIVED";
    }

    @Handler
    public void handleEvent(EventStoreForm.FormReceived event) {
        statuses.put(event.getFormId(), FormStatus.RECEIVED);
    }

    @Handler
    public void handleEvent(EventStoreForm.FormParsed event) {
        statuses.put(event.getFormId(), FormStatus.PARSED);
    }

    @Handler
    public void handleEvent(EventStoreForm.FormDelivered event) {
        statuses.put(event.getFormId(), FormStatus.DELIVERED);
    }

    public int getNumberOfForms() {
        return statuses.size();
    }

    public FormStatus getStatus(String formId) {
        return statuses.get(formId);
    }

    public Map<String, FormStatus> getStatuses(List<String> formIds) {
        Map<String, FormStatus> result = new HashMap<>();
        for (String formId : formIds){
            result.put(formId, statuses.get(formId));
        }
        return result;
    }
}
