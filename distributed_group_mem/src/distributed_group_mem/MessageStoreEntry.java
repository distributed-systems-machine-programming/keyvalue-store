package distributed_group_mem;

public class MessageStoreEntry {
	
	private String MessageID;
	private int fullMessagesize;
	private int currentMessagesize;
	private String MachineID;
	private byte[] message = new byte[1500];
	
	
	 MessageStoreEntry(String string, String string2, String string3, String string4,
			byte[] string5) {
		MessageID = string;
		fullMessagesize = Integer.parseInt(string2);
		currentMessagesize = Integer.parseInt(string3);
		MachineID = string4;
		message = string5;
	}

	public String getID(){return MessageID;}
	
	public boolean isMessageReady()
	{
		if(currentMessagesize == fullMessagesize)
				return true;
		else
			return false;
	}
	
	public byte[] getMessage() {return message;}
	
	public String getMachineID() {return MachineID;}

	public void update(MessageStoreEntry partMessage) {
		byte[] c = new byte[message.length + partMessage.getMessage().length];
		System.arraycopy(message, 0, c, 0, message.length);
		System.arraycopy(partMessage.getMessage(), 0, c, message.length, partMessage.getMessage().length);
		message = c;
		currentMessagesize += 1;
		
	}
}
