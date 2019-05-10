package no.ks.eventstore2.projection;

import akka.actor.ActorRef;
import eventstore.Messages;

class AwaitProjections {
    private String category;
    private long expiresAt;
    private Messages.StreamPosition awaitedPosition;
    private ActorRef awaitingActor;

    AwaitProjections(String category, long expiresAt, Messages.StreamPosition awaitedPosition, ActorRef awaitingActor) {
        this.category = category;
        this.expiresAt = expiresAt;
        this.awaitedPosition = awaitedPosition;
        this.awaitingActor = awaitingActor;
    }

    String getCategory() {
        return category;
    }

    long getExpiresAt() {
        return expiresAt;
    }

    Messages.StreamPosition getAwaitedPosition() {
        return awaitedPosition;
    }

    ActorRef getAwaitingActor() {
        return awaitingActor;
    }

    @Override
    public String toString() {
        return "AwaitProjections{" +
                "category='" + category + '\'' +
                ", expiresAt=" + expiresAt +
                ", awaitedPosition=" + awaitedPosition +
                ", awaitingActor=" + awaitingActor +
                '}';
    }
}
