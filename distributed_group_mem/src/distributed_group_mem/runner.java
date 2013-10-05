package distributed_group_mem;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
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
	
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
		
		//INITIALIZE THE PARAMETERS
		initParams();
		
		
		//ASSIGN MACHINE IDs
		String fullMachineID = getFullMachineID();
		String shortMachineID = getShortMachineID();
		
		//LOGGER SETUP 
		try {
		      LogWriter.setup(shortMachineID);
		    } catch (IOException e) {
		      e.printStackTrace();
		      throw new RuntimeException("Problems with creating the log files");
		    }
		final Logger LOGGER = Logger.getLogger(runner.class.getName());
		
		//CREATE MEMBERSHIP LIST
		MemberList memberList = new MemberList(fullMachineID);
		
		//INITIALIZE HEART
		
		Heart dil = new Heart(HeartRate, memberList, fullMachineID);
		
		//INITIALIZE FAILURE DETECTOR
		
		//FailureDetector fd = new FailureDetector();
		
		//INITIALIZE GOSSIP LISTENER
		int listenerPort;
		if (args.length < 2)
			 listenerPort= 9898;
		else
		{
			 listenerPort = Integer.parseInt(args[1]);
		}
		
		Gossiper gos_obj = new Gossiper(listenerPort, GossipSendingRate, memberList);
		gos_obj.gossip_listener();
		
		
		
		while(true)
		{
			System.out.println("Enter the command.");
			System.out.println(">");
			LOGGER.info(fullMachineID+" # "+"Getting input");
			input = br.readLine();
		}
		
		
		
		
		
		
	}
	
	private static void initParams()
	{
		 try {
			 
				File fXmlFile = new File("parameters.xml");
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);
			 
				doc.getDocumentElement().normalize();
			 
				System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			 
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
		System.out.println("key : " + key);
		System.out.println("value : " + value);
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
		
			long unixTime = System.currentTimeMillis() / 1000L;
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
	
	
	
	
}
