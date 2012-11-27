package no.ks.eventstore2.eventstore.command;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.command.CommandDispatcherFactory;
import no.ks.eventstore2.command.CommandHandlerFactory;
import no.ks.eventstore2.eventstore.testImplementations.NotificationCommandHandler;
import no.ks.eventstore2.eventstore.testImplementations.NotificationSendt;
import no.ks.eventstore2.eventstore.testImplementations.SendNotification;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static akka.testkit.JavaTestKit.duration;

public class CommandDispatcherTest extends TestKit {

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public CommandDispatcherTest() {
        super(_system);
    }

    @Test
    public void testSendsCommandToNotificationCommandHandler() throws Exception {
        CommandDispatcherFactory factory = new CommandDispatcherFactory();
        List<CommandHandlerFactory> factories = new ArrayList<CommandHandlerFactory>();
        factories.add(new CommandHandlerFactory() {

            @Override
            public Actor create() throws Exception {
                return new NotificationCommandHandler(eventStore);
            }
        });
        factory.setCommandHandlerFactories(factories);
        factory.setEventStore(super.testActor());
        TestActorRef commandDispatcherRef = TestActorRef.create(_system, new Props(factory), "commandDispatcherKing");

        commandDispatcherRef.tell(new SendNotification(), super.testActor());
        expectMsgClass(duration("300 seconds"),NotificationSendt.class);
    }
}
