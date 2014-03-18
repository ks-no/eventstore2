package no.ks.eventstore2.command;

import static akka.testkit.JavaTestKit.duration;

import java.util.ArrayList;
import java.util.List;

import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormParser;
import no.ks.eventstore2.formProcessorProject.ParseForm;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.Actor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;

import com.typesafe.config.ConfigFactory;

public class CommandDispatcherTest extends TestKit {

    private static Logger log = LoggerFactory.getLogger(CommandDispatcherTest.class);

    static ActorSystem _system = ActorSystem.create("TestSys", ConfigFactory
            .load().getConfig("TestSys"));

    public CommandDispatcherTest() {
        super(_system);
    }

    @Test
    public void testSendsCommandToNotificationCommandHandler() throws Exception {
        log.debug("Test started");
        CommandDispatcherFactory factory = new CommandDispatcherFactory();
        List<CommandHandlerFactory> factories = new ArrayList<CommandHandlerFactory>();
        factories.add(new CommandHandlerFactory() {
            public Actor create() throws Exception {
                return new FormParser(eventStore);
            }
        });
        factory.setCommandHandlerFactories(factories);
        factory.setEventStore(super.testActor());

        TestActorRef commandDispatcherRef = TestActorRef.create(_system, new Props(factory), "commandDispatcherKing");
        Thread.sleep(2000);
        commandDispatcherRef.tell(new ParseForm("1"), super.testActor());
        expectMsgClass(duration("300 seconds"), FormParsed.class);
    }
}
