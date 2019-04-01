package no.ks.eventstore2.formProcessorProject;

import akka.actor.ActorRef;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.ProtobufHelper;
import no.ks.eventstore2.command.CommandHandler;

import java.util.UUID;

public class FormDeliverer extends CommandHandler {
    public FormDeliverer(ActorRef eventStore) {
        super(eventStore);
    }

    @Handler
    public void handleCommand(DeliverForm command) {
        eventStore.tell(
                ProtobufHelper.newEventWrapper(
                        "form",
                        UUID.randomUUID().toString(),
                        EventStoreForm.FormDelivered.newBuilder().setFormId(command.getFormId()).build()),
                self());
    }
}
