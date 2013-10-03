package distributed_group_mem;

import java.util.ArrayList;

//A data structure to store all the partial messages received
public class MessageStore {
	
	ArrayList<MessageStoreEntry> Messages = new ArrayList<MessageStoreEntry>();
	
	
	public String push(String partMessage)
	{
		MessageStoreEntry temp = parseHeader(partMessage);
		if(fetchEntry(temp.getID()))
		{
			MessageStoreEntry temp1 = fetch(temp.getID());
			temp1.update(temp);
			if(temp1.isMessageReady())
			{
				return temp1.getMessage();
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
				return temp.getMessage();
			}
			else
			{
				Messages.add(temp);
				return null;
			}
		}
			
			
	}


	private MessageStoreEntry parseHeader(String partMessage) {
		String[] brokenmessage = partMessage.split("#$");
	
		return new MessageStoreEntry(brokenmessage[0],brokenmessage[1],brokenmessage[2],brokenmessage[3]);
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
