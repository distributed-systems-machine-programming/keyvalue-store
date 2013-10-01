package distributed_group_mem;

import java.io.*;
import java.net.*;

public class comminucationLayer {
	public void send() throws Exception
	{
		try{
		
			String sendString = null;  
			for (int i=0; i<2000; i++)
			  {
				  sendString += "re";
			  }
			  sendString += "I AM DONE";
			  sendString += '\0';
		      DatagramSocket clientSocket = new DatagramSocket();
		      
		      InetAddress IPAddress = InetAddress.getByName("172.16.206.85");
		      byte[] sendData = new byte[sendString.length()];
		      System.out.println(sendString);
		      byte[] receiveData = new byte[1024];
		      System.out.println(sendData.length);
		      sendData = sendString.getBytes();
		      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 9999);
		      clientSocket.send(sendPacket);
		      DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		      clientSocket.receive(receivePacket);
		      String modifiedSentence = new String(receivePacket.getData());
		      System.out.println("FROM SERVER:" + modifiedSentence);
		      clientSocket.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
