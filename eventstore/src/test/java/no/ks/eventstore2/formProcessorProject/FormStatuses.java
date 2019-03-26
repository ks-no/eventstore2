package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.ProjectionOld;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ListensTo(value = {FormReceived.class, FormParsed.class, FormDelivered.class}, aggregates = "FORM")
public class FormStatuses extends ProjectionOld {

    Map<String, FormStatus> statuses = new LinkedHashMap<String, FormStatus>();

    public FormStatuses(ActorRef eventStore) {
        super(eventStore);
    }

    public void handleEvent(FormReceived event){
        statuses.put(event.getFormId(), FormStatus.RECEIVED);
    }

    public void handleEvent(FormParsed event){
        statuses.put(event.getFormId(), FormStatus.PARSED);
    }

    public void handleEvent(FormDelivered event){
        statuses.put(event.getFormId(), FormStatus.DELIVERED);
    }

    public int getNumberOfForms() {
        return statuses.size();
    }

    public FormStatus getStatus(String formId) {
        return statuses.get(formId);
    }

    public Map<String, FormStatus> getStatuses(List<String> formIds){
        Map<String, FormStatus> result = new HashMap<String, FormStatus>();
        for (String formId : formIds){
            result.put(formId, statuses.get(formId));
        }
        return result;
    }
}
