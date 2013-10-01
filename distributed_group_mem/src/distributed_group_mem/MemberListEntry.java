package distributed_group_mem;

public class MemberListEntry {
	private String MachineID;
	private Long Heartbeat;
	private Long localTimeStamp;
	
	MemberListEntry(String MachineID)
	{
		this.MachineID = MachineID;
		Heartbeat= 0L;
		localTimeStamp = getCurrentTime();
	}
	
	public void updateHeartBeat (Long hb)
	{
		if(hb>this.Heartbeat)
		{
			this.Heartbeat = hb;
		}
	}
	
	public void updateLocalHeartBeat ()
	{
		Heartbeat = Heartbeat + 1;
	}
	
	public Long getHeartBeat() {return Heartbeat;}
	
	
	
	public String getMachineID() {return MachineID;}
	
	

	
	private Long getCurrentTime()
	{
		return  System.currentTimeMillis() / 1000L;
		
	}
	
}
