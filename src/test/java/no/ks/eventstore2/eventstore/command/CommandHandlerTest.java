package no.ks.eventstore2.eventstore.command;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.eventstore.testImplementations.NotificationCommandHandler;
import no.ks.eventstore2.eventstore.testImplementations.NotificationSendt;
import no.ks.eventstore2.eventstore.testImplementations.SendNotification;
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
        final TestActorRef<NotificationCommandHandler> ref = TestActorRef.create(_system, new Props(NotificationCommandHandler.class), "notification_handler");
        ReflectionTestUtils.setField(ref.underlyingActor(), "eventStore", super.testActor());

        ref.tell(new SendNotification(), super.testActor());
        expectMsgClass(NotificationSendt.class);
    }
}
