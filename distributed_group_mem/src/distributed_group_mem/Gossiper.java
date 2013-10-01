package distributed_group_mem;

import java.util.*;

public class Gossiper extends Thread{
	Messenger messenger = new Messenger();
	MemberList receivedList = null;
	int port;
	int GossipSendingRate;
	
	Gossiper(int port, int GossipSendingRate)
	{
		this.port = port;
		this.GossipSendingRate = GossipSendingRate;
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
				  messenger.sendMessage(ListofSendMachineIDs);
				  try {
					    Thread.sleep(GossipSendingRate);
					} catch(InterruptedException ex) {
					    Thread.currentThread().interrupt();
					}
			  }
			 
			  
		  }

		private ArrayList<String> getSenderList(int noOfSenders) {
			// TODO Auto-generated method stub
			return null;
		}

		private int getNoOfSenders() {
			// TODO Auto-generated method stub
			return 0;
		}
		};
	Thread gossipListenerThread = new Thread () {
		  public void run () {
			  receivedList = messenger.getMessage();
			  updateLocalMemList(receivedList);
		  }
		};
		

}
