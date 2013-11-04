
package distributed_group_mem;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.logging.Logger;

/*THIS CLASS CONTAINS ALL THE FUNCTIONALITY TO PERFORM GOSSIP AND LISTEN TO GOSSIP. 
 *IT CALLS THE CORRESPONDING METHODS TO MODIFY THE MEMBERLIST WHENEVER NEEDED. 
 *IT ALSO CONTAINS THE FAILURE DETECTOR.
 * 
 * 
 */

public class Messenger {
	private final static int BUFFER_SIZE = 1500; ///using 1500 as its the recommended safe size for a UDP packet
	private MemberList localMemberList = null;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();
	private DatagramSocket sendSocket = null;
	private DatagramSocket receiveSocket = null;
	private InetAddress sendAddress;
	private byte[] sendData = new byte[BUFFER_SIZE]; 
    private byte[] receiveData = new byte[BUFFER_SIZE];
    private byte[] tempData = new byte[BUFFER_SIZE];
    private int sendPort;
    private int listenerPort;
    private byte[] newMessage;
    private int messageCount = 0;
    private String localMachineID;
    private boolean joinConfirm = false;
    private int failureCleanUpRate;
    private int failureTimeOut;
    final Logger LOGGER = Logger.getLogger(runner.class.getName());
    private int lossRate;
    private int updateMessageCount=0;
    private int g;
    
	Messenger (int port, MemberList localList, String machineID, int failureCleanUpRate, int failureTimeOut, int lossRate) throws Exception
	{
		localMemberList = localList;
		listenerPort = port;
		this.lossRate = lossRate;
		try{
			sendSocket = new DatagramSocket();
			receiveSocket = new DatagramSocket(listenerPort);
			localMachineID = machineID;
			this.failureCleanUpRate =failureCleanUpRate; 
			this.failureTimeOut = failureTimeOut;
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to create sendSocket or receiveSocket.");
		}
		
	}

//CONVERT THE RECEIVED BYTE STREAM TO THE MEMBERLIST OBJECT AND GET THE REMOTE MEMBERSHIP LIST
	
	private MemberList getMemberListFromBytes(byte[] bytes) {
		
		MemberList temp = null;
		try{
        ByteArrayInputStream baos = new ByteArrayInputStream(bytes);
        ObjectInputStream oos = new ObjectInputStream(baos);
        temp = (MemberList)oos.readObject();
		}catch (IOException e) {
            e.printStackTrace();	            
	    } catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		}
		return temp;
	}	
	

//METHOD TO RECEIVE PACKETS
	
	public void getMessage()
	{
		
		while (true)
		{
			try{
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		      receiveSocket.receive(receivePacket);
		      joinConfirm = true;
		      byte[] type = new byte[1];
		      byte[] data = new byte[BUFFER_SIZE];
				System.arraycopy(receivePacket.getData(), 0, type, 0, 1);
				System.arraycopy(receivePacket.getData(), 1, data, 0, BUFFER_SIZE-1);
		      String sType = new String(type);
				//System.out.println(sType);
		      MemberList remoteData = getMemberListFromBytes(data);
		      //System.out.println(remoteData.Print());
		      String remoteMachineID = remoteData.getFullList().get(0).getMachineID();
		     // System.out.println(remoteMachineID);
		      
		      if(sType.equals("j"))
		      {
			      ArrayList<String> blah = new ArrayList<String>();
					 blah.add(remoteMachineID);
					 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
			      write.lock();
				  try {
					 // System.out.println("Before Add");
					  localMemberList.addEntry(remoteMachineID, remoteData);
					  LOGGER.info(localMachineID + " # " + remoteMachineID + " added to the group.");
					  LOGGER.fine(localMachineID + " # " + "Membership List after adding "+ remoteMachineID + ":" + localMemberList.Print());
					 // System.out.println("After Add"); 
				  } finally {
				    write.unlock();
				  
				    sendMessage(remoteMachineIPBlah, "update");
				    //System.out.println("Got some issues in trying to add to the membership list");
				  }
				  
			  }
		      else if(sType.equals("u"))
		      {
		    	  write.lock();
				  try {
					  LOGGER.info(localMachineID + " # " + remoteMachineID + " has sent an update request.");
					  LOGGER.fine(localMachineID + " # " + "Membership List before update:" + localMemberList.Print());
					  localMemberList.updateList(remoteMachineID, remoteData);
					  
					  LOGGER.fine(localMachineID + " # " + "Membership List after update:" + localMemberList.Print());	 
				  } finally {
				    write.unlock();
				      
				   // System.out.println("Got some issues in trying to update the membership list");
				  }
		      }
		      else if(sType.equals("l"))
		      {
		    	  write.lock();
				  try {
					  localMemberList.removeEntry(remoteMachineID);
					  LOGGER.info(localMachineID + " # " + remoteMachineID + " has left the group.");
					  LOGGER.fine(localMachineID + " # " + "Membership List after removing "+ remoteMachineID + ":" + localMemberList.Print());
				  } finally {
				    write.unlock();
				      
				   // System.out.println("Got some issues in trying to update the membership list");
				  }
		      }
		      
		      
			}catch (IOException e)
			{
				System.out.println("Listener unable to fetch data");
			}
		
		}
		}
		
		      
//METHOD TO SEND ANY TYPE OF MESSAGE
	
	public void sendMessage(ArrayList<String> listofSendMachineIPs, String messageType)
	{
		byte[] sendMessage = generateMessage(localMemberList);	
		ArrayList<byte[]> UDPreadymessages = addHeader(sendMessage, messageType);
		send(UDPreadymessages, listofSendMachineIPs, messageType);
		
		
	}

//APPEND THE MESSAGE WITH THE MESSAGE TYPE
	
	private ArrayList<byte[]> addHeader(byte[] sendMessage, String messageType) {
		ArrayList<byte[]> returnData = new ArrayList<byte[]>();
		String cType;
		byte[] bType = null;
		if(messageType.equals("update"))
		{
			cType = "u";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
			
		}
		if(messageType.equals("leave"))
		{
			cType = "l";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("join"))
		{
			cType = "j";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		byte[] c = new byte[bType.length + sendMessage.length];
		System.arraycopy(bType, 0, c, 0, bType.length);
		System.arraycopy(sendMessage, 0, c, bType.length, sendMessage.length);
		returnData.add(c);
		return returnData;
	}

	
	//RETRIEVE IP FROM MACHINE ID
	private ArrayList<String> getMachineIPsfromIDs(ArrayList<String> listofSendMachineIDs) {
		
		String[] parts;
		ArrayList <String> IPs = new ArrayList <String>();
		for (int i=0; i<listofSendMachineIDs.size(); i++)
		{
			parts = listofSendMachineIDs.get(i).split("\\+");
			IPs.add(parts[0]);
		}
			
		return IPs;
	}

	
	
// CONVERT THE MEMBERLIST OBJECT TO A BYTE STREAM USING JAVA OBJECT SERIALIZATION
	
	private byte[] generateMessage(MemberList localMemberList2) {
		read.lock();
		  try {
		    
              ByteArrayOutputStream bao = new ByteArrayOutputStream(BUFFER_SIZE);
        	  ObjectOutputStream oos = new ObjectOutputStream(bao);
        	  oos.writeObject(localMemberList2);     	   
              tempData = bao.toByteArray();
//              DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);               
//              socket.send(packet);			  
		  }	catch (IOException e) {
	            e.printStackTrace();	            
		    }
		  	finally {
		  		read.unlock();
		  		//System.out.println("Got some issues in trying to read the membership list");
		  	}
		return tempData;
	}



	private void send(ArrayList<byte[]> uDPreadymessages,ArrayList<String> listofSendMachineIPs, String messageType) 
	{
		for(int i=0; i<listofSendMachineIPs.size(); i++)
		{
			try {
					sendAddress = InetAddress.getByName(listofSendMachineIPs.get(i));
					for(int j=0; j<uDPreadymessages.size();j++)
					{
						sendData = uDPreadymessages.get(j);
						DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, sendAddress, listenerPort);
						sendSocket.send(sendPacket);
						messageCount++;
						//System.out.println("SizeofRequest" + String.valueOf(sendData.length));
						//System.out.println("MessageCount" + String.valueOf(messageCount));
						LOGGER.info(localMachineID + " # " + "Sent " + messageType + " request to " + listofSendMachineIPs.get(i));
					}
					
			}catch(UnknownHostException r)
			{
				System.out.println("Unable to create an InetAddress for " + listofSendMachineIPs.get(i));
			}
			catch(IOException r)
			{
				System.out.println("Unable to send data to" + listofSendMachineIPs.get(i));
			}
		}
		
	
	
	}

	//METHOD TO SEND LOCAL MEMBERLIST AT EVERY GOSSIP. THE "G" LOGIC TAKES CARE OF PACKET LOSS RATE
	public void sendLocalMemList() {
			
		if(g/100 == 0)
		{
			failureDetector();
		  ArrayList<String> ListofSendMachineIDs = getSenderList();
		  if(ListofSendMachineIDs.size() > 0)
		  {	  
			  ArrayList<String> listofSendMachineIPs = getMachineIPsfromIDs(ListofSendMachineIDs);
			  
			  sendMessage(listofSendMachineIPs, "update");
		  }
		  g = (++updateMessageCount%100)*lossRate;
		}
		else
		{
			g=0;
			updateMessageCount=0;
		}
		  
	}
	
	//THIS IS CALLED BEFORE EVERY GOSSIP IS SENT
	
	private void failureDetector() {

		
		ArrayList<String> IDsToMark = new ArrayList<String>();
		ArrayList<String> IDsToDelete = new ArrayList<String>();
		
		read.lock();
		try{
			int tableSize = this.getMessengerMemberList().getFullList().size();
			for (int i = 0; i < tableSize ; i++) {
				String deleteMachineID = this.getMessengerMemberList().getFullList().get(i).getMachineID();
				Long timestampDifference = this.getCurrentTime() - this.getMessengerMemberList().getFullList().get(i).getLocalTimeStamp();
				if( timestampDifference >= failureCleanUpRate)  {
					IDsToDelete.add(deleteMachineID);
					//System.out.println(deleteMachineID);
					FaultRateCalculator.notfalseDetections++;
					//System.out.println("notfalseDetections" + Integer.valueOf(FaultRateCalculator.notfalseDetections));
				}
				else if( timestampDifference >= failureTimeOut && timestampDifference < failureCleanUpRate )  {
					IDsToMark.add(deleteMachineID);
					//System.out.println(deleteMachineID);
					FaultRateCalculator.notfalseDetections++;
					//System.out.println("notfalseDetections" + Integer.valueOf(FaultRateCalculator.notfalseDetections));
				}
			} 
			
		}finally {
		    read.unlock();
		    //System.out.println("Got some issues in trying to delete the membership list");
		  }
		write.lock();
		try{
			for (int i=0; i<IDsToMark.size(); i++)
			{
				this.getMessengerMemberList().findEntry(IDsToMark.get(i)).setDeletionStatus(true);
				LOGGER.info(localMachineID + " # " + IDsToMark.get(i) + " has been marked for deletion.");
				  LOGGER.fine(localMachineID + " # " + "Membership List : " +  localMemberList.Print());
			}
				for (int j=0; j<IDsToDelete.size(); j++)
			{
				this.getMessengerMemberList().deleteEntry(IDsToDelete.get(j));
			LOGGER.warning(localMachineID + " # " + IDsToDelete.get(j) + " has failed.");
			LOGGER.info(localMachineID + " # " + IDsToDelete.get(j) + " has been removed from the Membership List.");
			  LOGGER.fine(localMachineID + " # " + "Membership List : " +  localMemberList.Print());
			}
		}finally{
			write.unlock();
		}

	}
	
	//GET RANDOM SENDERS FROM ALL THE REMOTE MACHINES PRESENT IN THE MEMBERLIST 
	private ArrayList<String> getSenderList() {
		int count=0;
		int noOfSenders=0;
		ArrayList<String> allIPs = new ArrayList<String>();
		ArrayList<String> senderIPs = new ArrayList<String>();
		read.lock();
		  try {
				for (int i=0; i<localMemberList.getSize(); i++)
				{
					if(localMemberList.getFullList().get(i).isAlive() && !localMemberList.getFullList().get(i).getMachineID().equals(localMachineID))
						{
							count++;
							allIPs.add(localMemberList.getFullList().get(i).getMachineIP());
						}
				}
				
		  } finally {
		    read.unlock();
		    //System.out.println("Got some issues in trying to read the membership list");
		  }
		  if(count < 2 && count > 0)
		  {
			  noOfSenders = 1;
		  }
		  else if(count >= 2)
		  {
			   noOfSenders = count/2;
		  }
		  Collections.shuffle(allIPs);
		  for(int i=0; i<noOfSenders; i++)
			  senderIPs.add(allIPs.get(i));
		  
		  return senderIPs;
		  
	
	}

	//METHOD TO SEND AND CONFIRM A JOIN
	public boolean sendJoinRequest(String contactIP) {
		
		ArrayList<String> ListofIPs = new ArrayList<String>();
		ListofIPs.add(contactIP);
		sendMessage(ListofIPs, "join");
		int i=0;
		boolean flag=false;
		while(!joinConfirm && i<10)
		{
		try{	
			Thread.sleep(100);
		}
		catch(Exception e)
		{
			System.out.println("Thread sleep error");
		}
			i++;
		}
		if(i<1000)
			flag=true;
		
		return flag;
		
		
	}

	
	//METHOD TO SEND LEAVE REQUEST
	public void sendLeaveRequest() {
		ArrayList<String> ListofMachineIDs = getSenderList();
		if(ListofMachineIDs.size() > 0)
		{
			
			ArrayList<String> listofSendMachineIPs = getMachineIPsfromIDs(ListofMachineIDs);
			sendMessage(listofSendMachineIPs, "leave");
		}
	}
	
	
	public MemberList getMessengerMemberList() {
		return localMemberList;
	}
	
	
	private Long getCurrentTime()
	{
		return  System.currentTimeMillis();
		
	}

	//CALLED BEFORE LEAVING
	public void closeSockets() {
		try{
		sendSocket.close();
		receiveSocket.close();
		}
		catch (Exception e)
		{
			System.out.println("Unable to close the sockets.");
		}
	}
	
}

