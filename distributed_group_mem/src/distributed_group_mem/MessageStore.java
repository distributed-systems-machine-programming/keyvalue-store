package distributed_group_mem;

import java.util.ArrayList;

//A data structure to store all the partially received messages

public class MessageStore {
	
	ArrayList<MessageStoreEntry> Messages = new ArrayList<MessageStoreEntry>();
	
	
	public byte[] push(byte[] partMessage)
	{
		MessageStoreEntry temp = parseHeader(partMessage);
		if(fetchEntry(temp.getID()))
		{
			MessageStoreEntry temp1 = fetch(temp.getID());
			temp1.update(temp);
			if(temp1.isMessageReady())
			{
				 byte[] tempSend = (" #$ #$ #$update#$"+temp1.getMachineID()).getBytes();
				 byte[] c = new byte[tempSend.length + temp1.getMessage().length];
					System.arraycopy(tempSend, 0, c, 0, tempSend.length);
					System.arraycopy(temp1.getMessage(), 0, c, tempSend.length, temp1.getMessage().length);
					return c;
				 
				 
			}
			else
			{
				return null;
			}
		}
		else
		{
			if(temp.isMessageReady())
			{
				byte[] tempSend = (" #$ #$ #$ #$"+temp.getMachineID()).getBytes();
				 byte[] c = new byte[tempSend.length + temp.getMessage().length];
					System.arraycopy(tempSend, 0, c, 0, tempSend.length);
					System.arraycopy(temp.getMessage(), 0, c, tempSend.length, temp.getMessage().length);
					return c;
			}
			else
			{
				Messages.add(temp);
				return null;
			}
		}
			
			
	}


	private MessageStoreEntry parseHeader(byte[] partMessage) {
		String[] brokenmessage = new String(partMessage).split("#$");
	
		return new MessageStoreEntry(brokenmessage[0],brokenmessage[1],brokenmessage[2],brokenmessage[3], brokenmessage[4].getBytes());
	}


	private MessageStoreEntry fetch(String id) {
		for (int i=0; i<Messages.size(); i++)
		{
			if(id.equals(Messages.get(i)))
			{
				return Messages.get(i);
			}
		}
		
		return null;
	}


	private boolean fetchEntry(String id) {
		for (int i=0; i<Messages.size(); i++)
		{
			if(id.equals(Messages.get(i)))
			{
				return true;
			}
		}
		return false;
	}
	
	
}
