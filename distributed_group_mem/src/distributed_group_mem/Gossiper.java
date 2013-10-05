
package distributed_group_mem;

import java.util.*;

public class Gossiper extends Thread{
	Messenger messenger = null;
	MemberList receivedList = null;
	int port;
	int GossipSendingRate;
	String mID;
	
	Gossiper(int port, int GossipSendingRate, MemberList localMemList, String mID) throws Exception
	{
		this.port = port;
		this.GossipSendingRate = GossipSendingRate;
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
				  int NoOfSenders = getNoOfSenders();
				  ArrayList<String> ListofSendMachineIDs = getSenderList(NoOfSenders);
				  messenger.sendMessage(ListofSendMachineIDs, "update");
				  try {
					    Thread.sleep(GossipSendingRate);
					} catch(InterruptedException ex) {
					    Thread.currentThread().interrupt();
					}
			  }
			 
			  
		  }

		private ArrayList<String> getSenderList(int noOfSenders) {
			// TODO get a list of random nodes that are not marked as failed
			return null;
		}

		private int getNoOfSenders() {
			// TODO write some algorithm to do this correctly
			return 0;
		}
		};
		
	Thread gossipListenerThread = new Thread () {
		  public void run () {
			  messenger.getMessage();
		  }
		};

	public void joinRequest(String string) {
		// TODO write code for join request
		
	}
	public void stopGossip() {
		// TODO Auto-generated method stub
		
	}
	public void stopGossipListener() {
		// TODO Auto-generated method stub
		
	}
		

}
