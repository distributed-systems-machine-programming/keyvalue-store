
package distributed_group_mem;

import java.io.Serializable;
import java.util.*;


//DATA STRUCTURE AND RELATED METHODS OF THE MEMBERLIST

public class MemberList implements Serializable {
	
	private ArrayList<MemberListEntry> memList = new ArrayList<MemberListEntry>();
	
	MemberList (String localMachineID)
	{
		memList.add(new MemberListEntry (localMachineID));
		
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
	
	public void addEntry(String incomingMachineID, MemberList ml) {
		
		this.memList.add(new MemberListEntry(incomingMachineID));
		int currentSize = this.memList.size()-1;		
		this.memList.get(currentSize).setMachineID(incomingMachineID);
		this.memList.get(currentSize).setHeartBeat(ml.memList.get(0).getHeartBeat());
		this.memList.get(currentSize).setlocalTimeStamp();  // To be confirmed.
		this.memList.get(currentSize).setDeletionStatus(false);	
		
	}

	public void removeEntry(String remoteMachineID) {
	
		for (int i = 0; i < this.memList.size(); i++) {
			if (this.memList.get(i).getMachineID().equals(remoteMachineID))
			{
				this.memList.get(i).setDeletionStatus(true);
			}
		}
		
	}
	public void deleteEntry(String remoteMachineID) {
		
		for (int i = 0; i < this.memList.size(); i++) {
			if (this.memList.get(i).getMachineID().equals(remoteMachineID))
			{
				this.memList.remove(i);
			}
		}
		
	}
	
	public void updateList(String remoteMachineID, MemberList incomingMemberList) {
		
		int localIndex=-1,otherIndex=0;
		String currentIP;
		int localMemberListsize = this.memList.size();
		int incomingMemberListsize = incomingMemberList.memList.size();
			
		for ( otherIndex = 0; otherIndex < incomingMemberListsize; otherIndex++ ) {
			currentIP = incomingMemberList.memList.get(otherIndex).getMachineID();

			for (int i = 0; i < localMemberListsize; i++ ) { // if localIndex == -1, entry not found in the local gossip table
				if (this.memList.get(i).getMachineID().equals(currentIP)){
					localIndex = i;
					break;
				}
				
			}

			if (localIndex >= 0 && incomingMemberList.memList.get(otherIndex).getHeartBeat() > this.memList.get(localIndex).getHeartBeat() && incomingMemberList.memList.get(otherIndex).getDeletionStatus() == false)
			{
				if(this.memList.get(localIndex).getDeletionStatus() == true) {
					this.memList.get(localIndex).setDeletionStatus(false);
					FaultRateCalculator.falseDetections++;
					//System.out.println("falseDetections" + Integer.valueOf(FaultRateCalculator.falseDetections));
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

	public ArrayList<MemberListEntry> getFullList() { return memList;	}
	
	public String Print()
	{
		String printString = new String();
		
		for (int i=0; i< memList.size(); i++)
		{
			printString += "\nFINE : ";
			printString += memList.get(i).getInline();
		}
		return printString;
	}
	
}

