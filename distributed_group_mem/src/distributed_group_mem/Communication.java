package distributed_group_mem;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Communication {

	public void send(String message) throws Exception
	{
		try{
		
			String sendString = null;  
			for (int i=0; i<10; i++)
			  {
				  sendString += "re";
			  }
			  sendString += "I AM DONE";
			  sendString += '\0';
		      DatagramSocket clientSocket = new DatagramSocket();
		      
		      InetAddress IPAddress = InetAddress.getByName("172.16.206.85");
		      byte[] sendData = new byte[sendString.length()];
		      System.out.println(message);
		      byte[] receiveData = new byte[1024];
		      System.out.println(sendData.length);
		      sendData = message.getBytes();
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
