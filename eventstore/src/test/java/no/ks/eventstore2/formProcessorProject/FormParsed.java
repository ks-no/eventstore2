package no.ks.eventstore2.formProcessorProject;

import no.ks.eventstore2.Event;

public class FormParsed extends Event {
    private String formId;

    public FormParsed(String formId) {
        this.formId = formId;
        this.aggregateId = "FORM";
    }

    public String getFormId() {
        return formId;
    }

    public void setFormId(String formId) {
        this.formId = formId;
    }

    @Override
    public String getLogMessage() {
        return "Form has been parsed";
    }
}
