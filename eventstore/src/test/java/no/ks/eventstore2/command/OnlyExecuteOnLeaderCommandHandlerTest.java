package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import no.ks.eventstore2.Handler;
import no.ks.eventstore2.formProcessorProject.ParseForm;
import no.ks.eventstore2.testkit.EventstoreEventstore2TestKit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class OnlyExecuteOnLeaderCommandHandlerTest extends EventstoreEventstore2TestKit {

    @DisplayName("In a non-cluster environment, an OnlyExecuteOnLeaderCommandHandler implementation should handle received commands")
    @Test
    void testOnlyExecuteOnLeaderCommandHandlerCommand() {
        final TestActorRef<OnlyExecuteOnLeaderCommandHandler> ref = TestActorRef.create(_system,
                Props.create(TestOnlyExecuteOnLeaderCommandHandler.class, super.testActor()));

        ref.tell(new ParseForm(UUID.randomUUID().toString()), super.testActor());
        expectMsg("PARSEFORM_RECEIVED");
    }

    private static class TestOnlyExecuteOnLeaderCommandHandler extends OnlyExecuteOnLeaderCommandHandler {
        public TestOnlyExecuteOnLeaderCommandHandler(ActorRef eventStore) {
            super(eventStore);
        }

        @Handler
        public void handleCommand(ParseForm command){
            eventStore.tell("PARSEFORM_RECEIVED", self());
        }
    }
}
