package distributed_group_mem;

import java.util.logging.Logger;


// HEART WHICH RUNS AS A SEPERATE THREAD TO INCREASE THE HEARTBEAT OF THE LOCAL MACHINE
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
			LOGGER.finer(MachineID + " # " + "Membership List" + memList.Print());
			localEntry.incrementHeartBeat();
			try {
			    Thread.sleep(HeartRate);
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
		}
	}
	
	
	
	
}
