package no.ks.eventstore2.formProcessorProject;

import no.ks.eventstore2.Event;

public class FormParsed extends Event {
	private static final long serialVersionUID = 1L;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FormParsed that = (FormParsed) o;

        if (formId != null ? !formId.equals(that.formId) : that.formId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return formId != null ? formId.hashCode() : 0;
    }
}
