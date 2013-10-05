
import java.io.Serializable;
import java.util.*;

public class MemberList implements Serializable {
	
	ArrayList<MemberListEntry> memList = new ArrayList<MemberListEntry>();
	
	MemberList (String localMachineID)
	{
		memList.add(new MemberListEntry (localMachineID));
		
	}

	public void incrementHeartBeat(String MachineID) {
		int localMemberListsize = this.memList.size();
		int localIndex=0;
		for (int i = 0; i < localMemberListsize; i++ ) { // if localIndex == -1, entry not found in the local gossip table
			if (this.memList.get(i).getMachineID().equals(MachineID))
				localIndex = i;
			else
				localIndex = -1;
		}
		
		if (localIndex>=0)
			this.memList.get(localIndex).incrementHeartBeat();
		
	}
	public int getSize() {
		return this.memList.size();
	}

	
	MemberListEntry findEntry (String MachineID)
	{
		MemberListEntry blank = new MemberListEntry("INVALID");
		for (int i=0; i<memList.size(); i++)
		{
			if (memList.get(i).getMachineID().equals(MachineID))
			{
				return memList.get(i);
			}
		}
		return blank;
	}
	
	public void mergeMemberList(MemberList incomingMemberList) {
		
		int localIndex=0,otherIndex=0;
		String currentIP;
		int localMemberListsize = this.memList.size();
		int incomingMemberListsize = incomingMemberList.memList.size();
			
		for ( otherIndex = 0; otherIndex < incomingMemberListsize; otherIndex++ ) {
			currentIP = incomingMemberList.memList.get(otherIndex).getMachineID();

			for (int i = 0; i < localMemberListsize; i++ ) { // if localIndex == -1, entry not found in the local gossip table
				if (this.memList.get(i).getMachineID().equals(currentIP))
					localIndex = i;
				else
					localIndex = -1;
			}

			if (localIndex >= 0 && incomingMemberList.memList.get(otherIndex).getHeartBeat() > this.memList.get(localIndex).getHeartBeat() && incomingMemberList.memList.get(otherIndex).getDeletionStatus() == false) {
				if(this.memList.get(localIndex).getDeletionStatus() == true) {
					this.memList.get(localIndex).setDeletionStatus(false);
				}			
				this.memList.get(localIndex).updateHeartBeat(incomingMemberList.memList.get(otherIndex).getHeartBeat());
				this.memList.get(localIndex).setlocalTimeStamp();
			}

			else if(localIndex < 0 && incomingMemberList.memList.get(otherIndex).getDeletionStatus() == false) {
				this.memList.add(new MemberListEntry(currentIP));
				int currentSize = this.memList.size()-1;
				this.memList.get(currentSize).setMachineID(currentIP);
				this.memList.get(currentSize).setHeartBeat(incomingMemberList.memList.get(otherIndex).getHeartBeat());
				this.memList.get(currentSize).setlocalTimeStamp();  // To be confirmed.
				this.memList.get(currentSize).setDeletionStatus(false);	
				
			}
		
		
	}
}
	
}

