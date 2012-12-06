package akka.cluster;

import no.ks.eventstore2.Event;

public class Increase extends Event {

    private String id="id";

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Increase() {
        aggregateId = "test";
    }

    @Override
    public String toString() {
        return "Increase{" +
                "id='" + id + '\'' +
                '}';
    }
}
