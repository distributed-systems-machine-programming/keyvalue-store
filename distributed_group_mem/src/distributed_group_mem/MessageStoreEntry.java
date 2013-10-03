package distributed_group_mem;

public class MessageStoreEntry {
	private String MessageID;
	private int fullMessagesize;
	private int currentMessagesize;
	private String message;
	
	
	 MessageStoreEntry(String string, String string2, String string3,
			String string4) {
		MessageID = string;
		fullMessagesize = Integer.parseInt(string2);
		currentMessagesize = Integer.parseInt(string3);
		message = string4;
	}

	public String getID(){return MessageID;}
	
	public boolean isMessageReady()
	{
		if(currentMessagesize == fullMessagesize)
				return true;
		else
			return false;
	}
	
	public String getMessage() {return message;}

	public void update(MessageStoreEntry partMessage) {
		message += partMessage.getMessage();
		currentMessagesize += 1;
		
	}
}
