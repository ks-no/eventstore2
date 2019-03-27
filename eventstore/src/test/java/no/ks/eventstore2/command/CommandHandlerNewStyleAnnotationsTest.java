package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Eventstore2TestKit;
import no.ks.eventstore2.Handler;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommandHandlerNewStyleAnnotationsTest extends Eventstore2TestKit {

    @Test
    public void test_that_a_command_handler_accepts_commands_annotated_with_handleCommand() throws Exception {
        TestActorRef<CommandHandlerWithNewStyleAnnotations> testActor = TestActorRef.create(_system, Props.create(CommandHandlerWithNewStyleAnnotations.class, super.testActor()), UUID.randomUUID().toString());
        testActor.tell(new TestCommand(), super.testActor());
        assertTrue(testActor.underlyingActor().commandReceived);
    }

    private static class CommandHandlerWithNewStyleAnnotations extends CommandHandler {

        public boolean commandReceived = false;

        public CommandHandlerWithNewStyleAnnotations(ActorRef eventStore) {
            super(eventStore);
        }

        @Handler
        public void handle(TestCommand command){
            commandReceived = true;
        }
    }

    private static class TestCommand extends Command {}
}
