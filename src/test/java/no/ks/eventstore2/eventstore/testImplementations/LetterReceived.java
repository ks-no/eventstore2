package no.ks.eventstore2.eventstore.testImplementations;

import no.ks.eventstore2.Event;

public class LetterReceived extends Event {

    private String letterId;

    public String getLetterId() {
        return letterId;
    }

    public void setLetterId(String forsendelseId) {
        this.letterId = forsendelseId;
    }
}
