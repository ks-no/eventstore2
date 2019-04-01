package no.ks.eventstore2.command;

import akka.actor.Props;
import akka.testkit.TestActorRef;
import eventstore.Messages;
import no.ks.events.svarut.Form.EventStoreForm;
import no.ks.eventstore2.testkit.Eventstore2TestKit;
import no.ks.eventstore2.formProcessorProject.FormParser;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class CommandHandlerTest extends Eventstore2TestKit {

    @Test
    void testCommandHandlerReceivesCommandAndDispatchesCorrespondingEvent() throws Exception {
        final TestActorRef<FormParser> ref = TestActorRef.create(_system, Props.create(FormParser.class, super.testActor()) ,"notification_handler");

        String formId = UUID.randomUUID().toString();
        ref.tell(new ParseForm(formId), super.testActor());

        Messages.EventWrapper eventWrapper = expectMsgClass(Duration.apply(3, TimeUnit.SECONDS), Messages.EventWrapper.class);
        EventStoreForm.FormParsed form = eventWrapper.getEvent().unpack(EventStoreForm.FormParsed.class);
        assertThat(form.getFormId(), is(formId));
    }
}
