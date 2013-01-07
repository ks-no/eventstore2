package no.ks.eventstore2.command;

import akka.ConfigurationException;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.MemberStatus;

public abstract class OnlyExecuteOnLeaderCommandHandler extends CommandHandler {

	private boolean leader = true;

	public OnlyExecuteOnLeaderCommandHandler(ActorRef eventStore) {
		super(eventStore);
	}

	@Override
	public void preStart() {
		updateLeaderState();
		subscribeToClusterEvents();
		super.preStart();
	}

	private void subscribeToClusterEvents() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			cluster.subscribe(self(), ClusterEvent.ClusterDomainEvent.class);
		} catch (ConfigurationException e) {
		}
	}

	private void updateLeaderState() {
		try {
			Cluster cluster = Cluster.get(getContext().system());
			boolean notReady = true;
			while(!cluster.readView().self().status().equals(MemberStatus.up())){
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {

				}
			}
			leader = cluster.readView().isLeader();
		} catch (ConfigurationException e) {
			leader = true;
		}
	}

	@Override
	public void onReceive(Object o) throws Exception {
		if( o instanceof ClusterEvent.LeaderChanged){
			updateLeaderState();
		}

		if(leader)
			super.onReceive(o);
	}
}
