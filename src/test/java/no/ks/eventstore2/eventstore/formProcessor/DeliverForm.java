package no.ks.eventstore2.eventstore.formProcessor;

import no.ks.eventstore2.command.Command;

public class DeliverForm extends Command {
    private String formId;

    public DeliverForm(String formId) {
        this.formId = formId;
    }

    public String getFormId() {
        return formId;
    }

    public void setFormId(String formId) {
        this.formId = formId;
    }
}
