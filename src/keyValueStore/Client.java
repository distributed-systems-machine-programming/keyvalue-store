package keyValueStore;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
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
		final static int altKeyvalPort = 6767;
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
		public static void getKeyValmessage(int key) {
			//String clientSentence;
			// String capitalizedSentence;
			ServerSocket welcomeSocket;
			try {
				welcomeSocket = new ServerSocket(altKeyvalPort);
			
		        
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
				      if(sType.equals("m"))
				      {
				      KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
				      if(receiveKV.identifier == -1)
				    	  System.out.println("Key not found in store.");
				      else
				    	  System.out.println("Result: "+String.valueOf(key) +" : "+ receiveKV.val.ID+ " "+receiveKV.val.name);
				      }
				      else if(sType.equals("n"))
				      {
				    	  System.out.println("Got Acknowledgement");
				      }
				      connectionSocket.close();
				      welcomeSocket.close();
			}
			catch(Exception e)
			{
				System.out.println("I made a huge mistake.");
			}
			
		        }
		public static void sendLookupMessage(byte[] message, String receiverIP, int key)  {
			//change implementation of this
			 //String sentence;
			 // String modifiedSentence;
			 
			  Socket clientSocket;
			try {
				clientSocket = new Socket(receiverIP, keyvalPort);
			
			  DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
			  DataInputStream inFromClient = new DataInputStream(clientSocket.getInputStream());
			  //sentence = "Testing keyval connection from " + String.valueOf(localIdentifier)+ " to " +String.valueOf(localSuccessor);
			  int len = message.length;
			  outToServer.writeInt(len);
			    if (len > 0) {
			    	outToServer.write(message, 0, len);
			    }
			    
			   /* int length = inFromClient.readInt();
		           System.out.println( String.valueOf(length));
		           byte[] temp_data = new byte[length];
		           if (length > 0) {
		               inFromClient.read(temp_data);
		           }
		           byte[] type = new byte[1];
				   byte[] data = new byte[len];
				   System.arraycopy(temp_data, 0, type, 0, 1);
				   System.arraycopy(temp_data, 1, data, 0, len-1);
				      String sType = new String(type);
				      System.out.println(sType);
				      KeyValEntry receiveKV = kvParseKeyValByteMessage(data);
				      receiveKV.print();
			  */
			  //modifiedSentence = inFromServer.readLine();
			 // System.out.println("FROM SERVER: " + modifiedSentence);
			  clientSocket.close();
			  getKeyValmessage(key);
			} catch (UnknownHostException e) {
				
				e.printStackTrace();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		private static KeyValEntry kvParseKeyValByteMessage(byte[] bytes) {
			
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
		public static void sendKeyValmessage(String message, String receiverIP)  {
			//change implementation of this
			 //String sentence;
			 // String modifiedSentence;
			 
			  Socket clientSocket;
			try {
				clientSocket = new Socket(receiverIP, keyvalPort);
			byte[] bmessage = message.getBytes();
			  DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
			  BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			  //sentence = "Testing keyval connection from " + String.valueOf(localIdentifier)+ " to " +String.valueOf(localSuccessor);
			  int len = bmessage.length;
			  outToServer.writeInt(len);
			    if (len > 0) {
			    	outToServer.write(bmessage, 0, len);
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
			else if(messageType.equals("handshake"))
			{
				cType = "h";
				bType = cType.getBytes();
				//System.out.println(String.valueOf(bType.length));
			}
			byte[] c = new byte[bType.length + sendMessage.length];
			System.arraycopy(bType, 0, c, 0, bType.length);
			System.arraycopy(sendMessage, 0, c, bType.length, sendMessage.length);
			
			return c;
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
        private static int getMbitIdentifier(int M, String key) {
                String hexHashOut = null;
                int identifier=0;
                try {
                        MessageDigest digest = MessageDigest.getInstance("SHA1");
                        digest.update(key.getBytes());
                        hexHashOut = getHex(digest.digest());
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
                        //System.out.println( bigTemp1.intValue() );


                        BigInteger BigIdentifier = intHashOut.mod(bigTemp1);
                        identifier = BigIdentifier.intValue();

                        //System.out.println("int Hash Out: " + intHashOut);
                        //System.out.println("divisor: " + String.valueOf(divisor));
                       // System.out.println("temp1: " + temp1);
                       // System.out.println("bigTemp1: " + bigTemp1);
                       // System.out.println("BigIdentifier: " + BigIdentifier);
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
    	
    	private static String getLocalIP() throws Exception
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

        public static void xx(String []args) throws Exception {
        	//String serverIP = "192.17.11.26";
        	analysis aiyo = new analysis();
        	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        	System.out.print("Enter Server IP: ");
            String serverIP = br.readLine();
            byte byteMessage1[] = addKeyValHeader("handshake",getLocalIP().getBytes()); 
            sendKeyValmessage(byteMessage1, serverIP);
        	while(true) {
                        System.out.println("\n--WELCOME TO KEY-VALUE STORE--\n");
                        //Scanner scanIn = new Scanner(System.in);        
                        String key = null;
                        String inputValue = null;
                        
                        
                        System.out.println("Enter the keyValue Operation : add | update | lookup | delete");
                        
                        String action = br.readLine();
                        //String action = scanIn.nextLine();
                        
                        if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("update") ){
                        
                        System.out.print("Enter the Key [Usage : <KEY> ] : ");
                        //key = scanIn.nextLine();
                        key=br.readLine();
                        System.out.print("Enter the Value [Usage : <ID> SPACE <NAME> : ");
                        //inputValue = scanIn.nextLine();
                        inputValue=br.readLine();
                        String []splitInput = inputValue.split(" ");
                        
                        int id = Integer.parseInt(splitInput[0]);
                        String name = splitInput[1];

                        int identifier = getMbitIdentifier(17, key);

                        KeyValEntry newKV = new KeyValEntry(identifier, new Value(id, name));
                        
                        byte byteMessage[] = addKeyValHeader(action, generateKeyValByteMessage(newKV));
                        long startTime = System.currentTimeMillis();
                        sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                        long stopTime = System.currentTimeMillis();
                        
                        aiyo.data.add(stopTime-startTime);
                        }
                        
                        else if (action.equalsIgnoreCase( "lookup") ) {
                                System.out.print("Enter the Key [Usage : <KEY> ] : ");
                                //key = scanIn.nextLine();
                                key = br.readLine();
                                int identifier = getMbitIdentifier(17, key);
                                byte byteMessage[] = addKeyValHeader(action,generateKeyValByteMessage(identifier));  
                                long startTime = System.currentTimeMillis();
                                sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                                long stopTime = System.currentTimeMillis();
                                
                                aiyo.data.add(stopTime-startTime);
                        }
                        else if (action.equalsIgnoreCase("delete")) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            key = br.readLine();
                            int identifier = getMbitIdentifier(17, key);
                            byte byteMessage[] = addKeyValHeader(action,generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                    }
                        
                                                
                        else if (action.equalsIgnoreCase("iadd")|| action.equalsIgnoreCase("iupdate")){
                        	System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier=Integer.parseInt(br.readLine());
                            System.out.print("Enter the Value [Usage : <ID> <SPACE> <NAME> : ");
                            //inputValue = scanIn.nextLine();
                            inputValue=br.readLine();
                            String []splitInput = inputValue.split(" ");
                            
                            int id = Integer.parseInt(splitInput[0]);
                            String name = splitInput[1];

                            //int identifier = getMbitIdentifier(17, key);

                            KeyValEntry newKV = new KeyValEntry(identifier, new Value(id, name));
                            
                            byte byteMessage[] = addKeyValHeader(action.substring(1), generateKeyValByteMessage(newKV));
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                        }
                        else if (action.equalsIgnoreCase( "ilookup") ) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier = Integer.parseInt(br.readLine());
                            byte byteMessage[] = addKeyValHeader(action.substring(1),generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                         }  
                        else if (action.equalsIgnoreCase("idelete")) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier = Integer.parseInt(br.readLine());
                            byte byteMessage[] = addKeyValHeader(action.substring(1),generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                         }  
                        else if (action.equalsIgnoreCase("exit") || action.equalsIgnoreCase("quit"))
                        {
                        	br.close();
                        	System.exit(0);
                        }
                        else
                        {
                        	System.out.println("Invalid Command");
                        }

                }
        	
        }
        public static void main(String []args) throws Exception {
        	//String serverIP = "192.17.11.26";
        	analysis aiyo = new analysis();
        	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        	System.out.print("Enter Server IP: ");
            String serverIP = br.readLine();
            byte byteMessage1[] = addKeyValHeader("handshake",getLocalIP().getBytes()); 
            sendKeyValmessage(byteMessage1, serverIP);
            int Min = 1;
            int Max = 1000000;
            int k;
        	for (int i=0; i<1000; i++) {
        		k = Min + (int)(Math.random() * ((Max - Min) + 1));
                        System.out.println("\n--WELCOME TO KEY-VALUE STORE--\n");
                        //Scanner scanIn = new Scanner(System.in);        
                        String key = null;
                        String inputValue = null;
                        
                        
                        System.out.println("Enter the keyValue Operation : add | update | lookup | delete");
                        
                        //String action = br.readLine();
                        //String action = scanIn.nextLine();
                        String action = "add";
                        if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("update") ){
                        
                        System.out.print("Enter the Key [Usage : <KEY> ] : ");
                        //key = scanIn.nextLine();
                        //key=br.readLine();
                        key = String.valueOf(k);
                        System.out.print("Enter the Value [Usage : <ID> SPACE <NAME> : ");
                        //inputValue = scanIn.nextLine();
                        //inputValue=br.readLine();
                        inputValue="100 temp";
                        String []splitInput = inputValue.split(" ");
                        
                        int id = Integer.parseInt(splitInput[0]);
                        String name = splitInput[1];

                        int identifier = getMbitIdentifier(17, key);

                        KeyValEntry newKV = new KeyValEntry(identifier, new Value(id, name));
                        
                        byte byteMessage[] = addKeyValHeader(action, generateKeyValByteMessage(newKV));
                        long startTime = System.currentTimeMillis();
                        sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                        long stopTime = System.currentTimeMillis();
                        
                        aiyo.data.add(stopTime-startTime);
                        }
                        
                        else if (action.equalsIgnoreCase( "lookup") ) {
                                System.out.print("Enter the Key [Usage : <KEY> ] : ");
                                //key = scanIn.nextLine();
                                //key = br.readLine();
                                key = String.valueOf(k);
                                int identifier = getMbitIdentifier(17, key);
                                byte byteMessage[] = addKeyValHeader(action,generateKeyValByteMessage(identifier));  
                                long startTime = System.currentTimeMillis();
                                sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                                long stopTime = System.currentTimeMillis();
                                
                                aiyo.data.add(stopTime-startTime);
                        }
                        else if (action.equalsIgnoreCase("delete")) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                           // key = br.readLine();
                            key = String.valueOf(k);
                            int identifier = getMbitIdentifier(17, key);
                            byte byteMessage[] = addKeyValHeader(action,generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, Integer.valueOf(key));
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                    }
                        
                                                
                        else if (action.equalsIgnoreCase("iadd")|| action.equalsIgnoreCase("iupdate")){
                        	System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier=Integer.parseInt(br.readLine());
                            System.out.print("Enter the Value [Usage : <ID> <SPACE> <NAME> : ");
                            //inputValue = scanIn.nextLine();
                            inputValue=br.readLine();
                            String []splitInput = inputValue.split(" ");
                            
                            int id = Integer.parseInt(splitInput[0]);
                            String name = splitInput[1];

                            //int identifier = getMbitIdentifier(17, key);

                            KeyValEntry newKV = new KeyValEntry(identifier, new Value(id, name));
                            
                            byte byteMessage[] = addKeyValHeader(action.substring(1), generateKeyValByteMessage(newKV));
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                        }
                        else if (action.equalsIgnoreCase( "ilookup") ) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier = Integer.parseInt(br.readLine());
                            byte byteMessage[] = addKeyValHeader(action.substring(1),generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                         }  
                        else if (action.equalsIgnoreCase("idelete")) {
                            System.out.print("Enter the Key [Usage : <KEY> ] : ");
                            //key = scanIn.nextLine();
                            int identifier = Integer.parseInt(br.readLine());
                            byte byteMessage[] = addKeyValHeader(action.substring(1),generateKeyValByteMessage(identifier));                                
                            long startTime = System.currentTimeMillis();
                            sendLookupMessage(byteMessage, serverIP, identifier);
                            long stopTime = System.currentTimeMillis();
                            
                            aiyo.data.add(stopTime-startTime);
                         }  
                        else if (action.equalsIgnoreCase("exit") || action.equalsIgnoreCase("quit"))
                        {
                        	br.close();
                        	System.exit(0);
                        }
                        else
                        {
                        	System.out.println("Invalid Command");
                        }

                }
        	aiyo.pushtofile();
        	
        }
       
        
}