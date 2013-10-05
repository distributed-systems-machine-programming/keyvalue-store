
package distributed_group_mem;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.locks.*;

public class Messenger implements Runnable {
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
    private int sendPort;
    private machineReferenceTable machineInfo = null;
    private MessageStore ms = null;
    private int listenerPort;
    private byte[] newMessage;
    private int messageCount;
    private String localMachineID;
    
	Messenger (int port, MemberList localList, String machineID) throws Exception
	{
		localMemberList = localList;
		listenerPort = port;
		try{
			sendSocket = new DatagramSocket();
			receiveSocket = new DatagramSocket(listenerPort);
			localMachineID = machineID;
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to create sendSocket or receiveSocket.");
		}
		
	}

	public void run()
	{
		 String[] parts = new String(newMessage).split("#$");
		 String remoteMachineID = parts[4];
		 ArrayList<String> blah = new ArrayList<String>();
		 blah.add(remoteMachineID);
		 MemberList remoteData = getMemberListFromBytes(parts[5].getBytes());
		 String command = parts[3];
		 if(command.equals("join"))
		 {
			 write.lock();
			  try {
				  localMemberList.addEntry(remoteMachineID, remoteData);
					 sendMessage(blah, "update");
			  } finally {
			    write.unlock();
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
			
		 
		
	}
	
	private MemberList getMemberListFromBytes(byte[] bytes) {
		// TODO Aswin's Code
		return null;
	}

	public MemberList getMessage()
	{
		
		while (true)
		{
			try{
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		      receiveSocket.receive(receivePacket);
		    
		      String[] parts = new String(receivePacket.getData()).split("#$");
				if(parts[3].equals("join"))
				{
					newMessage = receivePacket.getData();
					new Thread().start();
				}
				else if(parts[3].equals("leave"))
				{
					newMessage = receivePacket.getData();
					new Thread().start();
				}
				else if(parts[3].equals("update"))
				{
					newMessage = ms.push(receivePacket.getData());
					if(newMessage != null)
					{
						new Thread().start();
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
	
	

	public void sendMessage(ArrayList<String> listofSendMachineIDs, String messageType)
	{
		byte[] sendMessage = generateMessage(localMemberList);	//whatever will be the data structure of the buffered message. Kept it as string for now.
		ArrayList<byte[]> UDPreadymessages = generateUDPreadyMessage(sendMessage, messageType);
		ArrayList<String> listofSendMachineIPs = getMachineIPsfromIDs(listofSendMachineIDs);
		send(UDPreadymessages, listofSendMachineIPs);
	}

	

	private ArrayList<String> getMachineIPsfromIDs(ArrayList<String> listofSendMachineIDs) {
		
		String[] parts;
		ArrayList <String> IPs = null;
		for (int i=0; i<listofSendMachineIDs.size(); i++)
		{
			parts = listofSendMachineIDs.get(i).split("+");
			IPs.add(parts[0]);
		}
			
		return IPs;
	}

	private byte[] generateMessage(MemberList localMemberList2) {
		read.lock();
		  try {
		    //TODO Aswin's code comes here
		  } finally {
		    read.unlock();
		    System.out.println("Got some issues in trying to read the membership list");
		  }
		return null;
	}

	private ArrayList<byte[]> generateUDPreadyMessage(byte[] sendMessage, String messageType) {
		ArrayList<byte[]> allPackets = null;
		String messageID = localMachineID + "&" + String.valueOf(messageCount);
		String sHeader;
		byte[] bHeader;
		int noOfPackets = (sendMessage.length%BUFFER_SIZE)+1;
		if(messageType.equals("update"))
		{
			if(noOfPackets == 1)
			{
				sHeader = messageID + "#$" + String.valueOf("1") + "#$" + String.valueOf("1")+ "#$" + messageType+"#$"+localMachineID+ "#$";
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
					sHeader = messageID + "#$" + String.valueOf(noOfPackets) + "#$" + String.valueOf(i)+ "#$" + messageType+"#$"+localMachineID+ "#$";
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
			sHeader = messageID + "#$" + String.valueOf("1") + "#$" + String.valueOf("1")+ "#$" + messageType+"#$"+localMachineID+ "#$";
			bHeader = sHeader.getBytes();
			byte[] c = new byte[bHeader.length + sendMessage.length];
			System.arraycopy(bHeader, 0, c, 0, bHeader.length);
			System.arraycopy(sendMessage, 0, c, bHeader.length, sendMessage.length);
			allPackets.add(c);
			
		}
		else if(messageType.equals("leave"))
		{
			sHeader = messageID + "#$" + String.valueOf("1") + "#$" + String.valueOf("1")+ "#$" + messageType+"#$"+localMachineID+ "#$";
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
	
	
}

