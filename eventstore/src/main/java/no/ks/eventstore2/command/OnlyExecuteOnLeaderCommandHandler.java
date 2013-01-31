package no.ks.eventstore2.command;

import akka.actor.ActorRef;
import akka.cluster.ClusterEvent;
import no.ks.eventstore2.AkkaClusterInfo;

public abstract class OnlyExecuteOnLeaderCommandHandler extends CommandHandler {

    private AkkaClusterInfo akkaClusterInfo;

    public OnlyExecuteOnLeaderCommandHandler(ActorRef eventStore) {
		super(eventStore);
	}

	@Override
	public void preStart() {
        akkaClusterInfo = new AkkaClusterInfo(getContext().system());
        akkaClusterInfo.subscribeToClusterEvents(self());
		akkaClusterInfo.updateLeaderState();
		super.preStart();
	}



	@Override
	public void onReceive(Object o) throws Exception {
		if( o instanceof ClusterEvent.LeaderChanged){
			akkaClusterInfo.updateLeaderState();
		}

		if(akkaClusterInfo.isLeader() || "HandlesClasses".equals(o))
			super.onReceive(o);
	}
}
