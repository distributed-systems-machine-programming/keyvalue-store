package distributed_group_mem;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
public class runner {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws Exception 
	 */

	public static String getFullMachineID() throws Exception
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
	public static String getShortMachineID() throws Exception
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
	
	
	
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
		
		//get Machine IDs
		String fullMachineID = getFullMachineID();
		String shortMachineID = getShortMachineID();
		
		//setup and initialize logger
		try {
		      LogWriter.setup(shortMachineID);
		    } catch (IOException e) {
		      e.printStackTrace();
		      throw new RuntimeException("Problems with creating the log files");
		    }
		final Logger LOGGER = Logger.getLogger(runner.class.getName());
		

		
		while(true)
		{
			System.out.println("Enter the command.");
			System.out.println(">");
			LOGGER.info(fullMachineID+" # "+"Getting input");
			input = br.readLine();
		}
		
		
		
		
		
		
	}

}
