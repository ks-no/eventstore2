package no.ks.eventstore2.formProcessorProject;

import no.ks.eventstore2.projection.ListensTo;
import no.ks.eventstore2.projection.Projection;

import java.util.LinkedHashMap;
import java.util.Map;

@ListensTo(value = {FormReceived.class, FormParsed.class, FormDelivered.class}, aggregates = "FORM")
public class FormStatuses extends Projection {

    Map<String, FormStatus> statuses = new LinkedHashMap<String, FormStatus>();

    public int getNumberOfForms() {
        return statuses.size();
    }

    public FormStatus getStatus(String formId) {
        return statuses.get(formId);
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

}
