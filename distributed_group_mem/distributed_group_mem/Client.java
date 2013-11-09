package distributed_group_mem;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Scanner;

public class Client {
		final static int BUFFER_SIZE = 1500;
		final static int keyvalPort = 6789;
		public static void sendKeyValmessage(byte[] message, String receiverIP)  {
			//change implementation of this
			 //String sentence;
			 // String modifiedSentence;
			 
			  Socket clientSocket;
			try {
				clientSocket = new Socket(receiverIP, keyvalPort);
			
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
		static public byte[] addKeyValHeader(String messageType, byte[] sendMessage)
		{
			
			String cType;
			byte[] bType = null;
			if(messageType.equals("update"))
			{
				cType = "u";
				bType = cType.getBytes();
				//System.out.println(String.valueOf(bType.length));
				
			}
			if(messageType.equals("lookup"))
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
			byte[] c = new byte[bType.length + sendMessage.length];
			System.arraycopy(bType, 0, c, 0, bType.length);
			System.arraycopy(sendMessage, 0, c, bType.length, sendMessage.length);
			
			return c;
		}
		
		
        private static int getMbitIdentifier(int M, String key) {
                String hexHashOut = null;
                int identifier=0;
                try {
                        MessageDigest digest = MessageDigest.getInstance("SHA1");
                        digest.update(key.getBytes());
                        hexHashOut = runner.getHex(digest.digest());
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
       static  byte[] generateKeyValByteMessage(int identifier) {
        	byte[] tempData2 = new byte[BUFFER_SIZE]; 
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
    	
    	static byte[] generateKeyValByteMessage(KeyValEntry kv ) {
    		byte[] tempData2 = new byte[BUFFER_SIZE]; 
    	
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

        public static void main(String []args) throws IOException {
        	String serverIP = "192.17.11.26";
        	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        	while(true) {
                        System.out.println("@@@@@@@@######## WELCOME TO KEY-VALUE STORE ########@@@@@@@@");
                        //Scanner scanIn = new Scanner(System.in);        
                        String key = null;
                        String inputValue = null;
                       // System.out.println("Enter Server IP: ");
                       // String serverIP = scanIn.nextLine();
                        
                        System.out.println("Enter the keyValue Operation : ");
                        System.out.println("add");
                        System.out.println("update");
                        System.out.println("lookup");
                        System.out.println("delete" + "\n");
                        String action = br.readLine();
                        //String action = scanIn.nextLine();
                        
                        if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("update") ){
                        
                        System.out.print("Enter the Key [Usage : <KEY> ] : ");
                        //key = scanIn.nextLine();
                        key=br.readLine();
                        System.out.print("Enter the Value [Usage : <ID> <SPACE> <NAME> : ");
                        //inputValue = scanIn.nextLine();
                        inputValue=br.readLine();
                        String []splitInput = inputValue.split(" ");
                        
                        int id = Integer.parseInt(splitInput[0]);
                        String name = splitInput[1];

                        int identifier = getMbitIdentifier(17, key);

                        KeyValEntry newKV = new KeyValEntry(identifier, new Value(id, name));
                        
                        byte byteMessage[] = addKeyValHeader(action, generateKeyValByteMessage(newKV));
                        sendKeyValmessage(byteMessage, serverIP);
                        }
                        
                        if (action == "lookup") {
                                System.out.print("Enter the Key [Usage : <KEY> ] : ");
                                //key = scanIn.nextLine();
                                key = br.readLine();
                                int identifier = getMbitIdentifier(17, key);
                                byte byteMessage[] = addKeyValHeader(action,generateKeyValByteMessage(identifier));                                
                                
                        }
                                                
                        if (action == "delete") {
                                
                        }


                        //scanIn.close();            

                }
        }
}