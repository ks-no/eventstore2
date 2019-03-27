package no.ks.eventstore2.command;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import com.typesafe.config.ConfigFactory;
import no.ks.eventstore2.formProcessorProject.FormParsed;
import no.ks.eventstore2.formProcessorProject.FormParser;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static akka.testkit.JavaTestKit.duration;

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
        List<Props> props = new ArrayList<>();
        props.add(Props.create(FormParser.class, super.testActor()));
        TestActorRef commandDispatcherRef = TestActorRef.create(_system, CommandDispatcher.mkProps(props), "commandDispatcherKing");
        Thread.sleep(2000);
        commandDispatcherRef.tell(new ParseForm("1"), super.testActor());
        expectMsgClass(duration("300 seconds"), FormParsed.class);
    }
}
