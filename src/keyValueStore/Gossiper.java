
package keyValueStore;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;


/*THIS IS THE GOSSIPER CLASS WHICH BASICALLY CREATES THE THREADS TO RUN GOSSIP_LISTENER AND GOSSIP. 
 * ALL THE FUNCTIONALITY TO LISTEN AND SEND THE GOSSIP ARE WRITTEN IN THE MESSENGER CLASS
 */
public class Gossiper extends Thread{
	Messenger messenger = null;
	MemberList localMemList = null;
	int port;
	int keyvalPort;
	int GossipSendingRate;
	String mID;
	int m;
	int identifier;
	MapStore map;
	final Logger LOGGER = Logger.getLogger(runner.class.getName());
	
	Gossiper(int port, int GossipSendingRate, MemberList localMemList, String mID, int failureCleanUpRate, int failureTimeOut, int lossRate, int m, int identifier, MapStore map, int keyvalPort) throws Exception
	{
		this.port = port;
		this.GossipSendingRate = GossipSendingRate;
		this.localMemList = localMemList;
		this.mID = mID;
		messenger = new Messenger(port, localMemList,mID, failureCleanUpRate, failureTimeOut, lossRate, identifier, map, keyvalPort, m);
		this.m = m;
		this.identifier = identifier;
		this.map = map;
		this.keyvalPort = keyvalPort;
	}
	public void gossip_listener()
	{
		
		gossipListenerThread.start();
		
	}
	
	public void gossip()
	{
		gossipThread.start();
	}
	
	Thread keyvalListenerThread = new Thread()
	{
		public void run()
		{
			
				messenger.getKeyValmessage();
			
		}
	};
	
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


		private String getIPfromIdentifier(int identifier)
		{
			return localMemList.findEntry(identifier).getMachineIP();
		}
	public void joinRequest(String contactIP) {
		boolean success = messenger.sendJoinRequest(contactIP);
		if(success)
		{
			
			System.out.println("Joined the group successfully.");
			LOGGER.info(mID+" # "+"JOINED THE GROUP");
			/*try {
				//Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		
			messenger.getKeysFromSuccessor();
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
		messenger.sendKeysToSuccessor();
		messenger.sendLeaveRequest();
		
	}
	public void keyval_listener() {
		keyvalListenerThread.start();
		
	}
	
		
	
		

}
