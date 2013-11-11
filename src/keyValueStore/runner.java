package keyValueStore;


import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.logging.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;

import java.io.File;

public class runner {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws Exception 
	 */
	
	//all rates are in milli-seconds
	static int HeartRate;
	static int FailureCheckRate;
	static int FailureCleanUpRate;
	static int FailureTimeOut;
	static int GossipSendingRate;
	static String LoggingLevel;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
		
		//INITIALIZE THE PARAMETERS
		initParams();
		int listenerPort= 9898;
		int keyvalPort = 6789;
		
		//ASSIGN MACHINE IDs
		String fullMachineID = getFullMachineID();
		String shortMachineID = getShortMachineID();
		
		int m=17;
		
		//GET M-BIT IDENTIFIER
		
		int identifier = getMbitIdentifier(m);
		
		
		//LOGGER SETUP 
		try {
		      LogWriter.setup(shortMachineID, LoggingLevel);
		    } catch (IOException e) {
		      e.printStackTrace();
		      throw new RuntimeException("Problems with creating the log files");
		    }
		final Logger LOGGER = Logger.getLogger(runner.class.getName());
		
		//CREATE MEMBERSHIP LIST
		MemberList memberList = new MemberList(fullMachineID, identifier);
		MapStore map = null;
		if(args[0].equalsIgnoreCase("contact"))
		{
			 //map = new MapStore("sampleKeyValStore.xml");
			map = new MapStore();
		}
		else
		{
			 map = new MapStore();
		}
			//INITIALIZE HEART
		
		Heart dil = new Heart(HeartRate, memberList, fullMachineID);
		
		
		
		//INITIALIZE GOSSIP LISTENER
		int lossRate = Integer.parseInt(args[1]);
		
		Gossiper gos_obj = new Gossiper(listenerPort, GossipSendingRate, memberList, fullMachineID, FailureCleanUpRate, FailureTimeOut, lossRate, m, identifier, map, keyvalPort);
		gos_obj.gossip_listener();
		gos_obj.keyval_listener();
		if(args[0].equalsIgnoreCase("contact"))
		{
			gos_obj.gossip();
			
		}
		String[] temp;
		boolean firstTimeJoin = true;
		LOGGER.info(fullMachineID+" # "+"INITIALIZED");
		while(true)
		{
			System.out.println("\nUSAGE: join [contactIP] | leave | show | count");
			System.out.print(">");
			
			input = br.readLine();
			temp = input.split(" ");
			if(temp[0].equals("join"))
			{
				if(firstTimeJoin)
				{
					gos_obj.joinRequest(temp[1]);
					gos_obj.gossip();
					
					
					firstTimeJoin = false;
				}
				else
				{
					fullMachineID = getFullMachineID();
					shortMachineID = getShortMachineID();
					try {
					      LogWriter.setup(shortMachineID, LoggingLevel);
					    } catch (IOException e) {
					      e.printStackTrace();
					      throw new RuntimeException("Problems with creating the log files");
					    }
					memberList = new MemberList(fullMachineID, identifier);
					dil = new Heart(HeartRate, memberList, fullMachineID);
					gos_obj = new Gossiper(listenerPort, GossipSendingRate, memberList, fullMachineID, FailureCleanUpRate, FailureTimeOut, lossRate, m, identifier, map, keyvalPort);
					gos_obj.gossip_listener();
					gos_obj.joinRequest(temp[1]);
					gos_obj.gossip();
					
				}
				
				
			}
			else if(temp[0].equals("leave"))
			{
				LOGGER.info(fullMachineID+" # "+" LEFT");
				gos_obj.leaveRequest();
				gos_obj.stopGossip();
				gos_obj.stopGossipListener();
				dil.stop();
				System.exit(0);
			}
			else if(temp[0].equalsIgnoreCase("quit") | temp[0].equalsIgnoreCase("exit") )
			{
				LOGGER.info(fullMachineID+" # "+" CRASHED");
				System.exit(0);
				
			}
			else if(temp[0].equalsIgnoreCase("show") )
			{
				LOGGER.info(fullMachineID+" # "+" Print All Keys");
				map.printMapStore();
				
				
			}
			else if(temp[0].equalsIgnoreCase("count") )
			{
				map.count();
				
				
			}
			else
			{
				System.out.println("Invalid Command. Please enter again");
			}
		}
		
		
		
		
		
		
	}
	
	private static int getMbitIdentifier(int M) {
		String localIP=null;
		String hexHashOut = null;
		int identifier=0;
		try{
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()){
			    NetworkInterface current = interfaces.nextElement();
			    //System.out.println(current);
			    if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			    Enumeration<InetAddress> addresses = current.getInetAddresses();
			    while (addresses.hasMoreElements()){
			        InetAddress current_addr = addresses.nextElement();
			        if (current_addr.isLoopbackAddress()) continue;
			        if (current_addr instanceof Inet4Address)
			        	  localIP = current_addr.getHostAddress();
			        
			    }
			}
		
				
		}
		catch (SocketException e)
		{
			System.out.println("Unable to get Local Host Address");
			System.exit(1);
		}
		
		
		
		try {
	        MessageDigest digest = MessageDigest.getInstance("SHA1");
	        digest.update(localIP.getBytes());
	         hexHashOut = getHex (digest.digest());
	         System.out.println("Hex Hash Out: " + hexHashOut);
	        

	    } catch (Exception e) {
	        System.out.println("SHA1 not implemented in this system");
	    }
		
		try{
			BigInteger intHashOut = new BigInteger(hexHashOut,16);
			double divisor = Math.pow(2, M);
	        long temp1 = (long) divisor;
	        //BigInteger bigTemp1 = null;
	        //BigInteger.valueOf(temp1);
	     // Convert Long to String.
	        String stringVar =Long.toString(temp1);

	        // Convert to BigInteger. The BigInteger(byte[] val) expects a binary representation of 
	        // the number, whereas the BigInteger(string val) expects a decimal representation.
	        BigInteger bigTemp1 = new BigInteger( stringVar );

	        // See if the conversion worked. But the output from this step is not
	        // anything like the original value I put into longVar
	        System.out.println( bigTemp1.intValue() );
	        
	        
	       BigInteger BigIdentifier = intHashOut.mod(bigTemp1);
	       identifier = BigIdentifier.intValue();
	        
	        System.out.println("int Hash Out: " + intHashOut);
	        System.out.println("divisor: " + String.valueOf(divisor));
	        System.out.println("temp1: " + temp1);
	        System.out.println("bigTemp1: " + bigTemp1);
	        System.out.println("BigIdentifier: " + BigIdentifier);
	        System.out.println("identifier: " + identifier);
		}
		catch (Exception e)
		{
			System.out.println("Something");
		}
		return identifier;
		
	}

	private static void initParams()
	{
		 try {
			 
				File fXmlFile = new File("parameters.xml");
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);
			 
				doc.getDocumentElement().normalize();
			 
				//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			 System.out.println ("All values are in Milliseconds");
				NodeList nList = doc.getElementsByTagName("key");
			 
			
			 
				for (int temp = 0; temp < nList.getLength(); temp++) {
			 
					Node nNode = nList.item(temp);
			 			 
					if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			 
						Element eElement = (Element) nNode;
			 
						
						setParamValue(eElement.getAttribute("id"),eElement.getElementsByTagName("value").item(0).getTextContent());
									 
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
	}
	
	
	private static void setParamValue(String key, String value) {
		System.out.println(key + ": "+ value);
		//System.out.println("value : " + value);
		//key += '\0';
		try{
		if(key.matches("HeartRate"))
			HeartRate = Integer.parseInt(value);
		}catch (NullPointerException e)
		{
			//do nothing
		}
		try{
		if(key.matches("FailureCheckRate"))
			FailureCheckRate = Integer.parseInt(value);
		}catch (NullPointerException e)
		{
			//do nothing
		}
		try{
		if(key.matches("FailureCleanUpRate"))
			FailureCleanUpRate = Integer.parseInt(value);
		}catch (NullPointerException e)
		{
			//do nothing
		}
		try{
		if(key.matches("FailureTimeOut"))
			FailureTimeOut = Integer.parseInt(value);
		}catch (NullPointerException e)
		{
			//do nothing
		}
		try{
		if(key.matches("GossipSendingRate"))
			GossipSendingRate = Integer.parseInt(value);
		}catch (NullPointerException e)
		{
			//do nothing
		}	
		try{
			if(key.matches("Logging Level"))
				LoggingLevel = value;
			}catch (NullPointerException e)
			{
				//do nothing
			}
	}

	private static String getFullMachineID() throws Exception
	{
		String MachineID = null;
		String localIP=null;
		String timestamp=null;
		try{
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()){
			    NetworkInterface current = interfaces.nextElement();
			    //System.out.println(current);
			    if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			    Enumeration<InetAddress> addresses = current.getInetAddresses();
			    while (addresses.hasMoreElements()){
			        InetAddress current_addr = addresses.nextElement();
			        if (current_addr.isLoopbackAddress()) continue;
			        if (current_addr instanceof Inet4Address)
			        	  localIP = current_addr.getHostAddress();
			    }
			}
		
			long unixTime = System.currentTimeMillis();
			timestamp = Long.toString(unixTime);
			MachineID = localIP;
			MachineID += "+";
			MachineID += timestamp;
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to get Local Host Address");
			System.exit(1);
		}
		
		return MachineID;
	}
	private static String getShortMachineID() throws Exception
	{
		String MachineID = null;
		String localIP=null;
		String timestamp=null;
		try{
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()){
			    NetworkInterface current = interfaces.nextElement();
			    //System.out.println(current);
			    if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			    Enumeration<InetAddress> addresses = current.getInetAddresses();
			    while (addresses.hasMoreElements()){
			        InetAddress current_addr = addresses.nextElement();
			        if (current_addr.isLoopbackAddress()) continue;
			        if (current_addr instanceof Inet4Address)
			        	  localIP = current_addr.getHostAddress();
			    }
			}
		
			
			MachineID = localIP;
			
			
		}
		catch (SocketException e)
		{
			System.out.println("Unable to get Local Host Address");
			System.exit(1);
		}
		
		return MachineID;
	}
	
	 static final String HEXES = "0123456789ABCDEF";
	  public static String getHex( byte [] raw ) {
	    if ( raw == null ) {
	      return null;
	    }
	    final StringBuilder hex = new StringBuilder( 2 * raw.length );
	    for ( final byte b : raw ) {
	      hex.append(HEXES.charAt((b & 0xF0) >> 4))
	         .append(HEXES.charAt((b & 0x0F)));
	    }
	    return hex.toString();
	  }
	
	
}