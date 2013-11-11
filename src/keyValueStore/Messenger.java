
package keyValueStore;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
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
    private byte[] tempData2 = new byte[BUFFER_SIZE];
    private int sendPort;
    private int listenerPort;
    private int keyvalPort;
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
    private int localIdentifier;
    private int localSuccessor;
    private MapStore localMap;
    private String clientIP;
    private int m;
    private int altKeyvalPort = 6767;
	Messenger (int port, MemberList localList, String machineID, int failureCleanUpRate, int failureTimeOut, int lossRate, int identifier, MapStore map, int keyvalPort, int m) throws Exception
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
			localIdentifier = identifier;
			localMap = map;
			this.keyvalPort = keyvalPort;
			this.m = m;
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to create sendSocket or receiveSocket.");
		}
		
	}

//CONVERT THE RECEIVED BYTE STREAM TO THE MEMBERLIST OBJECT AND GET THE REMOTE MEMBERSHIP LIST
	
	public Messenger() {
		// TODO Auto-generated constructor stub
	}

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
				  findSuccessor(localIdentifier);
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
				  findSuccessor(localIdentifier);
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
	
	public void sendLocalMemList(String IP) {
		
			
		 
			  ArrayList<String> listofSendMachineIPs = new ArrayList<String>();
			  listofSendMachineIPs.add(IP);
			  
			  sendMessage(listofSendMachineIPs, "update");
		  
		  
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

	public byte[] addKeyValHeader(String messageType, byte[] sendMessage)
	{
		
		String cType;
		byte[] bType = null;
		if(messageType.equals("update"))
		{
			cType = "u";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
			
		}
		else if(messageType.equals("lookup"))
		{
			cType = "l";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("add"))
		{
			cType = "a";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("delete"))
		{
			cType = "d";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("getKeyVal"))
		{
			cType = "g";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("sendKeyVal"))
		{
			cType = "s";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("rightAdd"))
		{
			cType = "b";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("rightUpdate"))
		{
			cType = "v";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("rightDelete"))
		{
			cType = "e";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("rightLookup"))
		{
			cType = "k";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("leftLookup"))
		{
			cType = "j";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("clientLookup"))
		{
			cType = "m";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("ack"))
		{
			cType = "c";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		else if(messageType.equals("clientAck"))
		{
			cType = "n";
			bType = cType.getBytes();
			//System.out.println(String.valueOf(bType.length));
		}
		byte[] c = new byte[bType.length + sendMessage.length];
		System.arraycopy(bType, 0, c, 0, bType.length);
		System.arraycopy(sendMessage, 0, c, bType.length, sendMessage.length);
		
		return c;
	}
	
	

	public void sendKeysToSuccessor() {
		getAndSendKeyValsFromMap();
		
	}


	private String getIPfromIdentifier(int identifier)
	{
		return localMemberList.findEntry(identifier).getMachineIP();
	}
	
	public void findSuccessor(int identifier) {
		
		int[] allIdentifiers = new int[localMemberList.getSize()];
		int i=0;
		for(; i< localMemberList.getSize()-1; i++)
		{
			allIdentifiers[i] = localMemberList.getFullList().get(i+1).getIdentifier();
			
		}
		allIdentifiers[i] = identifier;
		Arrays.sort(allIdentifiers);
		int index = Arrays.binarySearch(allIdentifiers, identifier);
		int successorIndex;
		if(index == allIdentifiers.length-1)
			successorIndex = 0;
		else
			successorIndex = index+1;
		
		localSuccessor = allIdentifiers[successorIndex];
		//System.out.println(localSuccessor);
	}
public int findRightNode(int identifier) {
		
		int[] allIdentifiers = new int[localMemberList.getSize()+1];
		int i=0;
		for(; i< localMemberList.getSize(); i++)
		{
			allIdentifiers[i] = localMemberList.getFullList().get(i).getIdentifier();
			
		}
		allIdentifiers[i] = identifier;
		Arrays.sort(allIdentifiers);
		int index = Arrays.binarySearch(allIdentifiers, identifier);
		int successorIndex;
		if(index == allIdentifiers.length-1)
			successorIndex = 0;
		else
			successorIndex = index+1;
		
		return allIdentifiers[successorIndex];
		//System.out.println(localSuccessor);
	}

	public void sendKeyValmessage(byte[] message, String receiverIP)  {
		sendKeyValmessage(message, receiverIP, keyvalPort);
	}
	public void sendKeyValmessage(byte[] message, String receiverIP, int port)  {
		//change implementation of this
		 //String sentence;
		 // String modifiedSentence;
		 
		  Socket clientSocket;
		try {
			clientSocket = new Socket(receiverIP, port);
		
		  DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		  BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		  //sentence = "Testing keyval connection from " + String.valueOf(localIdentifier)+ " to " +String.valueOf(localSuccessor);
		  int len = message.length;
		  outToServer.writeInt(len);
		    if (len > 0) {
		    	outToServer.write(message, 0, len);
		    }
		  
		  
		  //modifiedSentence = inFromServer.readLine();
		 // System.out.println("FROM SERVER: " + modifiedSentence);
		  clientSocket.close();
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public void getKeyValmessage() {
		//String clientSentence;
		// String capitalizedSentence;
		ServerSocket welcomeSocket;
		try {
			welcomeSocket = new ServerSocket(keyvalPort);
		
	        while(true)
	        {
	           Socket connectionSocket;
			
				connectionSocket = welcomeSocket.accept();
			
	           DataInputStream inFromClient = new DataInputStream(connectionSocket.getInputStream());
	           int len = inFromClient.readInt();
	           System.out.println( String.valueOf(len));
	           byte[] temp_data = new byte[len];
	           if (len > 0) {
	               inFromClient.read(temp_data);
	           }
	           
	           //System.out.println(temp_data);
	           byte[] type = new byte[1];
			   byte[] data = new byte[len];
			   System.arraycopy(temp_data, 0, type, 0, 1);
			   System.arraycopy(temp_data, 1, data, 0, len-1);
			      String sType = new String(type);
			      System.out.println(sType);
			      //outToClient.writeBytes(capitalizedSentence);
			      if(sType.equals("g"))
			      {
			    	  int identifier = intParseKeyValByteMessage(data);
			    	  System.out.println(String.valueOf(identifier));
			    	  getAndSendKeyValsFromMap(identifier);
			    	
			      }
			      else if(sType.equals("s"))
			      {
			    	 KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	 receiveKV.print();
			    	 localMap.addEntry(receiveKV.identifier, receiveKV.val);
			      }
			      else if(sType.equals("a"))
			      {
			    	  KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  receiveKV.print();
			    	  int rightNode = findRightNode(receiveKV.identifier);
			    	  sendAddKeyValMessage(receiveKV, rightNode);
			    	  //localMap.addEntry(receiveKV.identifier, receiveKV.val);
			      }
		    	  else if(sType.equals("u"))
			      {
		    		  KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  receiveKV.print();
			    	  int rightNode = findRightNode(receiveKV.identifier);
			    	  sendUpdateKeyValMessage(receiveKV, rightNode);
			    	  
			      }
			      else if(sType.equals("l"))
			      {
			    	  int receiveKeyIdentifier = intParseKeyValByteMessage(data);
			    	  System.out.println(String.valueOf(receiveKeyIdentifier));
			    	  int rightNode = findRightNode(receiveKeyIdentifier);
			    	  sendLookupKeyValMessage(receiveKeyIdentifier, rightNode);
			      }
			      
			      else if(sType.equals("d"))
			      {
			    	  int receiveKeyIdentifier = intParseKeyValByteMessage(data);
			    	  System.out.println(String.valueOf(receiveKeyIdentifier));
			    	  int rightNode = findRightNode(receiveKeyIdentifier);
			    	  sendDeleteKeyValMessage(receiveKeyIdentifier, rightNode);
			      }
			      else if(sType.equals("b"))
			      {
			    	  String serverContactIP = connectionSocket.getInetAddress().toString();
			    	  serverContactIP = serverContactIP.substring(1);
			    	  System.out.println("serverContactIP: " + serverContactIP);
			    	  KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  receiveKV.print();
			    	  localMap.addEntry(receiveKV.identifier, receiveKV.val);
			    	  byte[] partMessage = generateKeyValByteMessage(9999);
			  		  byte[] fullMessage = addKeyValHeader("ack", partMessage);
			  		 sendKeyValmessage(fullMessage, serverContactIP);
			    	  
			      }
			      else if(sType.equals("v"))
			      {
			    	  String serverContactIP = connectionSocket.getInetAddress().toString();
			    	  serverContactIP = serverContactIP.substring(1);
			    	  System.out.println("serverContactIP: " + serverContactIP);
			    	  KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  receiveKV.print();
			    	  localMap.updateEntry(receiveKV.identifier, receiveKV.val);
			    	  byte[] partMessage = generateKeyValByteMessage(9999);
			  		  byte[] fullMessage = addKeyValHeader("ack", partMessage);
			  		 sendKeyValmessage(fullMessage, serverContactIP);
			    	  
			      }
			      else if(sType.equals("k"))
			      {
			    	  String serverContactIP = connectionSocket.getInetAddress().toString();
			    	  serverContactIP = serverContactIP.substring(1);
			    	  System.out.println("serverContactIP: " + serverContactIP);
			    	  int receiveKeyIdentifier = intParseKeyValByteMessage(data);
			    	  System.out.println(String.valueOf(receiveKeyIdentifier));
			    	  Value retVal = localMap.lookupEntry(receiveKeyIdentifier);
			    	  KeyValEntry sendKV;
			    	  if (retVal == null)
			    	  {
			    	   sendKV = new KeyValEntry(-1, new Value(141, "waste value"));
			    	  }
			    	  else
			    	  {
			    	   sendKV = new KeyValEntry(receiveKeyIdentifier, retVal);
			    	  }
			    	  byte[] partMessage = generateKeyValByteMessage(sendKV);
			  		  byte[] fullMessage = addKeyValHeader("leftLookup", partMessage);
			  		  sendKeyValmessage(fullMessage, serverContactIP);
			    	  
			    	  
			      }
			      else if(sType.equals("e"))
			      {
			    	  String serverContactIP = connectionSocket.getInetAddress().toString();
			    	  serverContactIP = serverContactIP.substring(1);
			    	  System.out.println("serverContactIP: " + serverContactIP);
			    	  int receiveKeyIdentifier = intParseKeyValByteMessage(data);
			    	  System.out.println(String.valueOf(receiveKeyIdentifier));
			    	  localMap.deleteEntry(receiveKeyIdentifier);
			    	  byte[] partMessage = generateKeyValByteMessage(9999);
			  		  byte[] fullMessage = addKeyValHeader("ack", partMessage);
			  		 sendKeyValmessage(fullMessage, serverContactIP);
			      }
			      else if(sType.equals("h"))
			      {
			    	  clientIP = connectionSocket.getInetAddress().toString();
			    	  clientIP = clientIP.substring(1);
			    	  System.out.println("ClientIP: " + clientIP);
			      }
			      else if(sType.equals("j"))
			      {
			    	  KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  receiveKV.print();
			    	  byte[] partMessage = generateKeyValByteMessage(receiveKV);
			  		  byte[] fullMessage = addKeyValHeader("clientLookup", partMessage);
			  		  sendKeyValmessage(fullMessage, clientIP, altKeyvalPort);
			      }
			      else if(sType.equals("c"))
			      {
			    	  //KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
			    	  //receiveKV.print();
			    	  //byte[] partMessage = generateKeyValByteMessage(receiveKV);
			  		  byte[] fullMessage = addKeyValHeader("clientAck", data);
			  		  sendKeyValmessage(fullMessage, clientIP, altKeyvalPort);
			      }
			      
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
     }

	private void sendUpdateKeyValMessage(KeyValEntry receiveKV, int rightNode) {
		byte[] partMessage = generateKeyValByteMessage(receiveKV);
		byte[] fullMessage = addKeyValHeader("rightUpdate", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(rightNode));
		
	}

	private void sendDeleteKeyValMessage(int receiveKeyIdentifier, int rightNode) {
		byte[] partMessage = generateKeyValByteMessage(receiveKeyIdentifier);
		byte[] fullMessage = addKeyValHeader("rightDelete", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(rightNode));
		
	}

	private void sendLookupKeyValMessage(int receiveKeyIdentifier, int rightNode) {
		byte[] partMessage = generateKeyValByteMessage(receiveKeyIdentifier);
		byte[] fullMessage = addKeyValHeader("rightLookup", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(rightNode));
		
	}

	private void sendAddKeyValMessage(KeyValEntry receiveKV, int rightNode) {
		byte[] partMessage = generateKeyValByteMessage(receiveKV);
		byte[] fullMessage = addKeyValHeader("rightAdd", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(rightNode));
		
	}

	private void getAndSendKeyValsFromMap(int identifier) {
		Map<Integer, Value> temp = localMap.getKeys();
		ArrayList<Integer> keys = new ArrayList<Integer>();
		int newLocalIdentifier = getRelativeIdentifier(localIdentifier, identifier);
		for (Entry<Integer, Value> entry : temp.entrySet())
		{
			
			int newIdentifier = getRelativeIdentifier(entry.getKey(), identifier);
			KeyValEntry x = new KeyValEntry (entry.getKey(), entry.getValue());
			if(newIdentifier>newLocalIdentifier)
			{	System.out.println("Identifier: " + String.valueOf(identifier));
			System.out.println("Identifier's IP: " + String.valueOf(getIPfromIdentifier(identifier)));
			System.out.println("MembershipList: " + localMemberList.Print());
				x.print();
				byte[] partMessage = generateKeyValByteMessage(x);
				byte[] fullMessage = addKeyValHeader("sendKeyVal", partMessage);
				sendKeyValmessage(fullMessage, getIPfromIdentifier(identifier));
				keys.add(entry.getKey());
			}
			
		
		}
		for(int i=0; i<keys.size();i++) {
			localMap.deleteEntry(keys.get(i));
		}
		
		
		
		
		/*ArrayList<Integer> keys = new ArrayList<Integer>();
		
		NavigableMap<Integer, Value> temp = localMap.getKeys(identifier);
		for(Integer key : temp.keySet()) {
            Value value = temp.get(key);
            System.out.printf("%s = %s%n", key, value);
            KeyValEntry x = new KeyValEntry (key, temp.get(key));
			byte[] partMessage = generateKeyValByteMessage(x);
			byte[] fullMessage = addKeyValHeader("sendKeyVal", partMessage);
			x.print();
			sendKeyValmessage(fullMessage, getIPfromIdentifier(identifier));
			keys.add(key);
        }
		for(int i=0; i<keys.size();i++) {
			localMap.deleteEntry(keys.get(i));
		}
		/*for (Entry<Integer, Value> entry : temp.entrySet())
		{
			KeyValEntry x = new KeyValEntry (entry.getKey(), entry.getValue());
			byte[] partMessage = generateKeyValByteMessage(x);
			byte[] fullMessage = addKeyValHeader("sendKeyVal", partMessage);
			x.print();
			//sendKeyValmessage(fullMessage, getIPfromIdentifier(identifier));
			localMap.deleteEntry(entry.getKey());
		}*/
		
		
		
		
	}

private int getRelativeIdentifier(int identifier, int reference) {
		
		int i1 = identifier-reference;
		double size = Math.pow(2, m);
		if(i1<=0)
		{
			return (int) (size+i1);
		}
		return i1;
	}

private void getAndSendKeyValsFromMap() {
		Map<Integer, Value> temp = localMap.getKeys();
		ArrayList<Integer> keys = new ArrayList<Integer>();
		
		for (Entry<Integer, Value> entry : temp.entrySet())
		{
			byte[] partMessage = generateKeyValByteMessage(new KeyValEntry (entry.getKey(), entry.getValue()));
			byte[] fullMessage = addKeyValHeader("sendKeyVal", partMessage);
			sendKeyValmessage(fullMessage, getIPfromIdentifier(localSuccessor));
			keys.add(entry.getKey());
		}
		for(int i=0; i<keys.size();i++) {
			localMap.deleteEntry(keys.get(i));
		}
	}
	
	
	public void getKeysFromSuccessor() {
		sendLocalMemList(getIPfromIdentifier(localSuccessor));
		byte[] partMessage = generateKeyValByteMessage(localIdentifier);
		byte[] fullMessage = addKeyValHeader("getKeyVal", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(localSuccessor));
	}

	
	public void sendKeyVals(int remoteidentifier)
	{
		byte[] partMessage = generateKeyValByteMessage(localIdentifier);
		byte[] fullMessage = addKeyValHeader("sendKeyVal", partMessage);
		sendKeyValmessage(fullMessage, getIPfromIdentifier(remoteidentifier));
		
	}
	
	 byte[] generateKeyValByteMessage(int identifier) {
		try {
		ByteArrayOutputStream bao = new ByteArrayOutputStream(BUFFER_SIZE);
  	    ObjectOutputStream oos = new ObjectOutputStream(bao);
  	   
		oos.writeObject(identifier);
		tempData2 = bao.toByteArray();
		} catch (IOException e) {
		
		e.printStackTrace();
	}     	   
        
		return tempData2;
	}
	
	byte[] generateKeyValByteMessage(KeyValEntry kv ) {
		try {
			ByteArrayOutputStream bao = new ByteArrayOutputStream(BUFFER_SIZE);
	  	    ObjectOutputStream oos = new ObjectOutputStream(bao);
	  	   
			oos.writeObject(kv);
			tempData2 = bao.toByteArray();
			} catch (IOException e) {
			
			e.printStackTrace();
		}     	   
	        
			return tempData2;
	}
	private int intParseKeyValByteMessage(byte[] bytes) {
		
		int temp = 0;
		try{
        ByteArrayInputStream baos = new ByteArrayInputStream(bytes);
        ObjectInputStream oos = new ObjectInputStream(baos);
        temp = (Integer)oos.readObject();
		}catch (IOException e) {
            e.printStackTrace();	            
	    } catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		}
		return temp;
	}		
	
private KeyValEntry kvParseKeyValByteMessage(byte[] bytes) {
		
		KeyValEntry temp = null;
		try{
        ByteArrayInputStream baos = new ByteArrayInputStream(bytes);
        ObjectInputStream oos = new ObjectInputStream(baos);
        temp = (KeyValEntry)oos.readObject();
		}catch (IOException e) {
            e.printStackTrace();	            
	    } catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		}
		return temp;
	}
	
}

