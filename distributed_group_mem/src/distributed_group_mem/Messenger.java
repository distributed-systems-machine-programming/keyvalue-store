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

public class Messenger {
	private MemberList localMemberList = null;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();
	private DatagramSocket sendSocket = null;
	private DatagramSocket receiveSocket = null;
	private InetAddress sendAddress;
	private byte[] sendData = new byte[1500]; //using 1500 as its the recommended safe size for a UDP packet
    private byte[] receiveData = new byte[1500];
    private int sendPort;
    private machineReferenceTable machineInfo = null;
    private MessageStore ms = null;
    private int listenerPort;
    
	Messenger (int port, MemberList localList) throws Exception
	{
		localMemberList = localList;
		listenerPort = port;
		try{
			sendSocket = new DatagramSocket();
			receiveSocket = new DatagramSocket(listenerPort);
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to create sendSocket or receiveSocket.");
		}
		
	}

	public MemberList getMessage()
	{
		return null;
		
	}
	
	public void sendMessage(ArrayList<String> listofSendMachineIDs)
	{
		String sendMessage = generateMessage(localMemberList);	//whatever will be the data structure of the buffered message. Kept it as string for now.
		ArrayList<String> UDPreadymessages = generateUDPreadyMessage(sendMessage);
		ArrayList<String[]> listofSendMachineIPs = getMachineIPsfromIDs(listofSendMachineIDs);
		send(UDPreadymessages, listofSendMachineIPs);
	}

	

	private ArrayList<String[]> getMachineIPsfromIDs(
			ArrayList<String> listofSendMachineIDs) {
		// TODO fetch machine info table to get IPs and Hosts
		return null;
	}

	private String generateMessage(MemberList localMemberList2) {
		read.lock();
		  try {
		    //TODO Aswin's code comes here
		  } finally {
		    read.unlock();
		    System.out.println("Got some issues in trying to read the membership list");
		  }
		return null;
	}

	private ArrayList<String> generateUDPreadyMessage(String sendMessage) {
		// TODO breaking down of messages if they are too big. Also append headers to them
		return null;
	}

	private void send(ArrayList<String> uDPreadymessages,ArrayList<String[]> listofSendMachineIPs) 
	{
		for(int i=0; i<listofSendMachineIPs.size(); i++)
		{
			try {
					sendAddress = InetAddress.getByName(listofSendMachineIPs.get(i)[0]);
					sendPort = Integer.parseInt(listofSendMachineIPs.get(i)[1]);
					for(int j=0; j<uDPreadymessages.size();j++)
					{
						sendData = uDPreadymessages.get(j).getBytes();
						DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, sendAddress, sendPort);
						sendSocket.send(sendPacket);
					}
					
			}catch(UnknownHostException r)
			{
				System.out.println("Unable to create an InetAddress for " + listofSendMachineIPs.get(i)[0]);
			}
			catch(IOException r)
			{
				System.out.println("Unable to send data to" + listofSendMachineIPs.get(i)[0]);
			}
		}
		
	
	
	}
	
	
}
