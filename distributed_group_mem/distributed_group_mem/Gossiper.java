
package distributed_group_mem;

import java.util.*;
import java.util.logging.Logger;


/*THIS IS THE GOSSIPER CLASS WHICH BASICALLY CREATES THE THREADS TO RUN GOSSIP_LISTENER AND GOSSIP. 
 * ALL THE FUNCTIONALITY TO LISTEN AND SEND THE GOSSIP ARE WRITTEN IN THE MESSENGER CLASS
 */
public class Gossiper extends Thread{
	Messenger messenger = null;
	MemberList localMemList = null;
	int port;
	int GossipSendingRate;
	String mID;
	final Logger LOGGER = Logger.getLogger(runner.class.getName());
	
	Gossiper(int port, int GossipSendingRate, MemberList localMemList, String mID, int failureCleanUpRate, int failureTimeOut, int lossRate) throws Exception
	{
		this.port = port;
		this.GossipSendingRate = GossipSendingRate;
		this.localMemList = localMemList;
		this.mID = mID;
		messenger = new Messenger(port, localMemList,mID, failureCleanUpRate, failureTimeOut, lossRate);
	}
	public void gossip_listener()
	{
		
		gossipListenerThread.start();
		
	}
	
	public void gossip()
	{
		gossipThread.start();
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

	public void joinRequest(String contactIP) {
		boolean success = messenger.sendJoinRequest(contactIP);
		if(success)
		{
			System.out.println("Joined the group successfully.");
			LOGGER.info(mID+" # "+"JOINED THE GROUP");
		}
		else
		{
			System.out.println("Unable to join the group.");
		}
		
	}
	@SuppressWarnings("deprecation")
	public void stopGossip() {
		gossipThread.stop();
		
	}
	@SuppressWarnings("deprecation")
	public void stopGossipListener() {
		messenger.closeSockets();
		gossipListenerThread.stop();
		
	}
	public void leaveRequest() {
		messenger.sendLeaveRequest();
		
	}
	public int findSuccessor(int identifier) {
		int successor=0;
		int gap=2000000;
		int index = -1;
		for(int i=0; i< localMemList.getSize(); i++)
		{
			if(localMemList.getFullList().get(i).getIdentifier()- identifier < gap && localMemList.getFullList().get(i).getIdentifier() - identifier>0)
			{
				gap = localMemList.getFullList().get(i).getIdentifier() - identifier;
				index = i;
				
			}
		}
		
		successor = localMemList.getFullList().get(index).getIdentifier();
		
		return successor;
		
	}
	public void getKeysFromSuccessor() {
		// TODO Auto-generated method stub
		
	}
		

}
