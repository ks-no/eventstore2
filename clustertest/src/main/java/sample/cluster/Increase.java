package sample.cluster;

import no.ks.eventstore2.Event;

public class Increase extends Event {

    public Increase() {
        aggregateId = "test";
    }
}
