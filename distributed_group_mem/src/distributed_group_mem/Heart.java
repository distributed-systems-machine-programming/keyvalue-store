package distributed_group_mem;

import java.util.logging.Logger;

public class Heart extends Thread{
	int HeartRate;
	MemberList memList;
	String MachineID;
	MemberListEntry localEntry;
	final Logger LOGGER = Logger.getLogger(runner.class.getName());
	
	Heart(int HeartbeatRate, MemberList ml, String localMachineID)
	{
		HeartRate = HeartbeatRate;
		memList = ml;
		MachineID = localMachineID;
		localEntry = memList.findEntry(MachineID);
		start();
	}
	
	public void run()
	{
		while(true)
		{
			LOGGER.fine(MachineID + " # " + "HeartBeat value - " + Long.toString(localEntry.getHeartBeat()));
			localEntry.updateLocalHeartBeat();
			try {
			    Thread.sleep(HeartRate);
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
		}
	}
	
	
	
	
}
