
package distributed_group_mem;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.logging.Logger;

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
    private MessageStore ms = null;
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

	/*public void run()
	{
		 String[] parts = new String(newMessage).split("---,,,");
		 String remoteMachineID = parts[4];
		 
		 ArrayList<String> blah = new ArrayList<String>();
		 blah.add(remoteMachineID);
		 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
		 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
		 String command = parts[3];
		 if(command.equals("join"))
		 {
			 write.lock();
			  try {
				  localMemberList.addEntry(remoteMachineID, remoteData);
					 
			  } finally {
			    write.unlock();
			  
			    sendMessage(remoteMachineIPBlah, "update");
			    System.out.println("Got some issues in trying to add to the membership list");
			  }
		 }
		 else if(command.equals("leave"))
		 {
			 write.lock();
			  try {
				  localMemberList.removeEntry(remoteMachineID);
					
			  } finally {
			    write.unlock();
			    System.out.println("Got some issues in trying to remove from the membership list");
			  }
		 }
		 else if(command.equals("update"))
		 {
			 write.lock();
			  try {
				  localMemberList.updateList(remoteMachineID, remoteData);
				
			  } finally {
			    write.unlock();
			    System.out.println("Got some issues in trying to update the membership list");
			  }
		 }
			
		 
		
	}*/
	
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
	


/*	public MemberList getMessage()

	{
		
		while (true)
		{
			try{
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		      receiveSocket.receive(receivePacket);
		    
		      String[] brrr = new String(receivePacket.getData()).split("---,,,");
				if(brrr[3].equals("join"))
				{
					joinConfirm = true;
					newMessage = receivePacket.getData();
					String[] parts = new String(newMessage).split("---,,,");
					 String remoteMachineID = parts[4];
					 
					 ArrayList<String> blah = new ArrayList<String>();
					 blah.add(remoteMachineID);
					 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
					 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
					 
					 write.lock();
					  try {
						  localMemberList.addEntry(remoteMachineID, remoteData);
							 
					  } finally {
					    write.unlock();
					  
					    sendMessage(remoteMachineIPBlah, "update");
					    System.out.println("Got some issues in trying to add to the membership list");
					  }
					
				}
				else if(brrr[3].equals("leave"))
				{
					newMessage = receivePacket.getData();
					
				}
				else if(brrr[3].equals("update"))
				{
					newMessage = ms.push(receivePacket.getData());
					try{
					if(!newMessage.equals(null))
					{
						String[] parts = new String(newMessage).split("---,,,");
						 String remoteMachineID = parts[4];
						 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
						
						 write.lock();
						  try {
							  localMemberList.updateList(remoteMachineID, remoteData);
							
						  } finally {
						    write.unlock();
						    System.out.println("Got some issues in trying to update the membership list");
						  }
					}
					}
					catch(NullPointerException e)
					{
						//do nothing and move on
					}
				}
		      
		      //Each message handling should be done on a seperate thread.parse the socket
		      //if it is a join request, then add an entry in the memList
		      //if it an update request, then update all the relevant entries in the memList
		      //if it is a leave request, then remove the entry in the memList
		      //make appropriate logs where ever necessary
		      
			}catch (IOException e)
			{
				System.out.println("Listener unable to fetch data");
			}
		}
		
	}
	
	*/
	
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
		
		      

	public void sendMessage(ArrayList<String> listofSendMachineIPs, String messageType)
	{
		byte[] sendMessage = generateMessage(localMemberList);	
		//ArrayList<byte[]> UDPreadymessages = generateUDPreadyMessage(sendMessage, messageType);
		ArrayList<byte[]> UDPreadymessages = addHeader(sendMessage, messageType);
		send(UDPreadymessages, listofSendMachineIPs, messageType);
		//send (sendMessage, listofSendMachineIPs);
		//addHeader(sendMessage, "join");
		//addHeader(sendMessage, "update");
		//addHeader(sendMessage, "leave");
	}

	

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

	private void send(byte[] sendMessage, ArrayList<String> listofSendMachineIPs) {
		for(int i=0; i<listofSendMachineIPs.size(); i++)
		{
			try {
					sendAddress = InetAddress.getByName(listofSendMachineIPs.get(i));
					
						sendData = sendMessage;
						DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, sendAddress, listenerPort);
						sendSocket.send(sendPacket);
					
					
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

	private ArrayList<byte[]> generateUDPreadyMessage(byte[] sendMessage, String messageType) {
		ArrayList<byte[]> allPackets = new ArrayList<byte[]>();
		String messageID = localMachineID + "&" + String.valueOf(messageCount);
		String sHeader;
		byte[] bHeader;
		int noOfPackets = (sendMessage.length/BUFFER_SIZE)+1;
		if(messageType.equals("update"))
		{
			if(noOfPackets == 1)
			{
				sHeader = messageID + "---,,," + String.valueOf("1") + "---,,," + String.valueOf("1")+ "---,,," + messageType+"---,,,"+localMachineID+ "---,,,";
				bHeader = sHeader.getBytes();
				byte[] c = new byte[bHeader.length + sendMessage.length];
				System.arraycopy(bHeader, 0, c, 0, bHeader.length);
				System.arraycopy(sendMessage, 0, c, bHeader.length, sendMessage.length);
				allPackets.add(c);
			}
			else if(noOfPackets > 1)
			{
				for(int i=1; i<=noOfPackets; i++)
				{
					sHeader = messageID + "---,,," + String.valueOf(noOfPackets) + "---,,," + String.valueOf(i)+ "---,,," + messageType+"---,,,"+localMachineID+ "---,,,";
					bHeader = sHeader.getBytes();
					byte[] c = new byte[bHeader.length + BUFFER_SIZE];
					System.arraycopy(bHeader, 0, c, 0, bHeader.length);
					System.arraycopy(sendMessage, (i-1)*BUFFER_SIZE, c, (bHeader.length + (i-1)*BUFFER_SIZE), BUFFER_SIZE);
					allPackets.add(c);
				}
			}
		}
		else if(messageType.equals("join"))
		{
			sHeader = messageID + "---,,," + String.valueOf("1") + "---,,," + String.valueOf("1")+ "---,,," + messageType+"---,,,"+ localMachineID + "---,,,";
			bHeader = sHeader.getBytes();
			byte[] c = new byte[bHeader.length + sendMessage.length];
			System.arraycopy(bHeader, 0, c, 0, bHeader.length);
			System.arraycopy(sendMessage, 0, c, bHeader.length, sendMessage.length);
			allPackets.add(c);
			
		}
		else if(messageType.equals("leave"))
		{
			sHeader = messageID + "---,,," + String.valueOf("1") + "---,,," + String.valueOf("1")+ "---,,," + messageType+"---,,,"+ localMachineID + "---,,,";
			bHeader = sHeader.getBytes();
			allPackets.add(bHeader);
			
		}
		messageCount++;
		return allPackets;
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
	
	private void failureDetector() {

		// TODO Aswin's code for Failure Detector
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
					System.out.println("notfalseDetections" + Integer.valueOf(FaultRateCalculator.notfalseDetections));
				}
				else if( timestampDifference >= failureTimeOut && timestampDifference < failureCleanUpRate )  {
					IDsToMark.add(deleteMachineID);
					//System.out.println(deleteMachineID);
					FaultRateCalculator.notfalseDetections++;
					System.out.println("notfalseDetections" + Integer.valueOf(FaultRateCalculator.notfalseDetections));
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

