package no.ks.eventstore2.formProcessorProject;

import no.ks.eventstore2.Event;

public class FormReceived extends Event {
	private static final long serialVersionUID = 1L;

	private String formId;

    public FormReceived(String formId) {
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
        return "Form has been received";
    }
}
