
package distributed_group_mem;

import java.io.Serializable;


//DATA STRUCTURE AND RELATED METHODS OF AN INDIVIDUAL ENTRY IN THE MEMBERLIST

public class MemberListEntry implements Serializable {
	private String MachineID;
	private Long Heartbeat;
	private Long localTimeStamp;
	private boolean markForDeletion;
	
	MemberListEntry(String MachineID)
	{
		this.MachineID = MachineID;
		Heartbeat= 0L;
		localTimeStamp = getCurrentTime();
		markForDeletion = false;
	}
	public Long getHeartBeat() {return Heartbeat;}

	public void incrementHeartBeat() {
		this.Heartbeat += 1;
		this.localTimeStamp=getCurrentTime();
	}
	
	public void updateHeartBeat (Long hb)
	{
		if(hb>this.Heartbeat)
		{
			this.Heartbeat = hb;
		}
	}
	
	public void setHeartBeat (Long hb) {
		this.Heartbeat = hb;
	}	
	
	public String getMachineID() {return MachineID;}
	
	public boolean isAlive(){return !markForDeletion;}
	
	public void setMachineID(String MachineID) {
		this.MachineID = MachineID;
	}
	
	public boolean getDeletionStatus() {return markForDeletion;}
	
	public void setDeletionStatus(boolean value) { this.markForDeletion = value; }
		
	private Long getCurrentTime()
	{
		return  System.currentTimeMillis();
		
	}
	public void setlocalTimeStamp() {
		this.localTimeStamp = getCurrentTime();
	}
	public Long getLocalTimeStamp() {
		return this.localTimeStamp;
	}
	public String getMachineIP() {
		String[] breakMessage = MachineID.split("\\+");
		return breakMessage[0];
		
	}
	
	public String getInline(){
		return MachineID + " " + String.valueOf(Heartbeat) + " " + String.valueOf(localTimeStamp)+" " + String.valueOf(markForDeletion);
	}
	
	
}
