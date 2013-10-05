
package distributed_group_mem;

import java.util.*;

public class Gossiper extends Thread{
	Messenger messenger = null;
	MemberList localMemList = null;
	int port;
	int GossipSendingRate;
	String mID;
	
	Gossiper(int port, int GossipSendingRate, MemberList localMemList, String mID) throws Exception
	{
		this.port = port;
		this.GossipSendingRate = GossipSendingRate;
		this.localMemList = localMemList;
		this.mID = mID;
		messenger = new Messenger(port, localMemList,mID);
	}
	public void gossip_listener()
	{
		
		gossipListenerThread.start();
		
	}
	
	public void gossip()
	{
		gossipThread.start();
	}
	private void updateLocalMemList(MemberList receivedList2) {
		
		
	}
	
	Thread gossipThread = new Thread () {
		  public void run () {
			  while(true)
			  {
				  
				  messenger.sendLocalMemList();
				 
				  try {
					    Thread.sleep(GossipSendingRate);
					} catch(InterruptedException ex) {
					    Thread.currentThread().interrupt();
					}
			  }
			 
			  
		  }

	};
		
	Thread gossipListenerThread = new Thread () {
		  public void run () {
			  messenger.getMessage();
		  }
		};

	public void joinRequest(String IPListFileName) {
		messenger.sendJoinRequest(IPListFileName);
		
	}
	public void stopGossip() {
		gossipThread.interrupt();
		
	}
	public void stopGossipListener() {
		gossipListenerThread.interrupt();
		
	}
		

}
