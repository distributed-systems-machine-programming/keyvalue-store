
package distributed_group_mem;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;

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
    private int messageCount;
    private String localMachineID;
    private boolean joinConfirm = false;
    private int failureCleanUpRate;
    private int failureTimeOut;
  
    
	Messenger (int port, MemberList localList, String machineID, int failureCleanUpRate, int failureTimeOut) throws Exception
	{
		localMemberList = localList;
		listenerPort = port;
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
		 String[] parts = new String(newMessage).split("-");
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
	


	/*public MemberList getMessage()

	{
		
		while (true)
		{
			try{
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		      receiveSocket.receive(receivePacket);
		    
		      String[] brrr = new String(receivePacket.getData()).split("-");
				if(brrr[3].equals("join"))
				{
					joinConfirm = true;
					newMessage = receivePacket.getData();
					String[] parts = new String(newMessage).split("-");
					 String remoteMachineID = parts[4];
					 
					 ArrayList<String> blah = new ArrayList<String>();
					 blah.add(remoteMachineID);
					 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
					 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
					 String command = parts[3];
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
						String[] parts = new String(newMessage).split("-");
						 String remoteMachineID = parts[4];
						 
						 ArrayList<String> blah = new ArrayList<String>();
						 blah.add(remoteMachineID);
						 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
						 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
						 String command = parts[3];
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
		      MemberList remoteData = getMemberListFromBytes(receivePacket.getData());
		      
		      String remoteMachineID = remoteData.getFullList().get(0).getMachineID();
		      ArrayList<String> blah = new ArrayList<String>();
				 blah.add(remoteMachineID);
				 ArrayList<String> remoteMachineIPBlah =  getMachineIPsfromIDs(blah);
		      write.lock();
			  try {
				  localMemberList.addEntry(remoteMachineID, remoteData);
					 
			  } finally {
			    write.unlock();
			  
			    sendMessage(remoteMachineIPBlah, "update");
			    System.out.println("Got some issues in trying to add to the membership list");
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
		ArrayList<byte[]> UDPreadymessages = generateUDPreadyMessage(sendMessage, messageType);
		
		//send(UDPreadymessages, listofSendMachineIPs);
		send (sendMessage, listofSendMachineIPs);
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
		ArrayList <String> IPs = null;
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
		    
              ByteArrayOutputStream bao = new ByteArrayOutputStream(1024);
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
		  		System.out.println("Got some issues in trying to read the membership list");
		  	}
		return tempData;
	}

	private ArrayList<byte[]> generateUDPreadyMessage(byte[] sendMessage, String messageType) {
		ArrayList<byte[]> allPackets = null;
		String messageID = localMachineID + "&" + String.valueOf(messageCount);
		String sHeader;
		byte[] bHeader;
		int noOfPackets = (sendMessage.length/BUFFER_SIZE)+1;
		if(messageType.equals("update"))
		{
			if(noOfPackets == 1)
			{
				sHeader = messageID + "-" + String.valueOf("1") + "-" + String.valueOf("1")+ "-" + messageType+"-"+localMachineID+ "-";
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
					sHeader = messageID + "-" + String.valueOf(noOfPackets) + "-" + String.valueOf(i)+ "-" + messageType+"-"+localMachineID+ "-";
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
			sHeader = messageID + "-" + String.valueOf("1") + "-" + String.valueOf("1")+ "-" + messageType+"-"+localMachineID+ "-";
			bHeader = sHeader.getBytes();
			byte[] c = new byte[bHeader.length + sendMessage.length];
			System.arraycopy(bHeader, 0, c, 0, bHeader.length);
			System.arraycopy(sendMessage, 0, c, bHeader.length, sendMessage.length);
			allPackets.add(c);
			
		}
		else if(messageType.equals("leave"))
		{
			sHeader = messageID + "-" + String.valueOf("1") + "-" + String.valueOf("1")+ "-" + messageType+"-"+localMachineID+ "-";
			bHeader = sHeader.getBytes();
			allPackets.add(bHeader);
			
		}
		messageCount++;
		return allPackets;
	}

	private void send(ArrayList<byte[]> uDPreadymessages,ArrayList<String> listofSendMachineIPs) 
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
		
		  ArrayList<String> ListofSendMachineIDs = getSenderList();
		  ArrayList<String> listofSendMachineIPs = getMachineIPsfromIDs(ListofSendMachineIDs);
		  failureDetector();
		  sendMessage(listofSendMachineIPs, "update");
		
		
	}
	
	private void failureDetector() {

		// TODO Aswin's code for Failure Detector

		read.lock();
		try{
			int tableSize = this.getMessengerMemberList().getFullList().size();
			for (int i = 0; i < tableSize ; i++) {
				String deleteMachineID = this.getMessengerMemberList().getFullList().get(i).getMachineID();
				Long timestampDifference = this.getCurrentTime() - this.getMessengerMemberList().getFullList().get(i).getLocalTimeStamp();
				if( timestampDifference >= failureCleanUpRate)  {
					this.getMessengerMemberList().getFullList().remove(i);
					System.out.println(deleteMachineID);
					//Log the Entry.
				}
				else if( timestampDifference >= failureTimeOut && timestampDifference < failureCleanUpRate )  {
					this.getMessengerMemberList().getFullList().get(i).setDeletionStatus(true);
					System.out.println(deleteMachineID);
					//Log the Entry
				}
			} 
		}finally {
		    read.unlock();
		    System.out.println("Got some issues in trying to delete the membership list");
		  }

	}
	private ArrayList<String> getSenderList() {
		int count=0;
		
		ArrayList<String> allIPs = null;
		ArrayList<String> senderIPs = null;
		read.lock();
		  try {
				for (int i=0; i<localMemberList.getSize(); i++)
				{
					if(localMemberList.getFullList().get(i).isAlive())
						{
							count++;
							allIPs.add(localMemberList.getFullList().get(i).getMachineIP());
						}
				}
				
		  } finally {
		    read.unlock();
		    System.out.println("Got some issues in trying to read the membership list");
		  }
		  int noOfSenders = count/2;
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
		while(!joinConfirm && i<1000)
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
		ArrayList<String> listofSendMachineIPs = getMachineIPsfromIDs(ListofMachineIDs);
		sendMessage(listofSendMachineIPs, "leave");
		
	}
	public MemberList getMessengerMemberList() {
		return localMemberList;
	}
	private Long getCurrentTime()
	{
		return  System.currentTimeMillis() / 1000L;
		
	}
	
}

