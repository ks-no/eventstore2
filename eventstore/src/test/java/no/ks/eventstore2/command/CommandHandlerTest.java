package no.ks.eventstore2.command;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormParser;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class CommandHandlerTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public CommandHandlerTest() {
        super(_system);
    }

    @Test
    public void testCommandHandlerReceivesCommandAndDispatchesCorrespondingEvent() throws Exception {
        final TestActorRef<FormParser> ref = TestActorRef.create(_system, Props.create(FormParser.class, super.testActor()) ,"notification_handler");
        ReflectionTestUtils.setField(ref.underlyingActor(), "eventStore", super.testActor());
        ref.tell(new ParseForm("formId"), super.testActor());
        expectMsgClass(FormParsed.class);
    }
}
