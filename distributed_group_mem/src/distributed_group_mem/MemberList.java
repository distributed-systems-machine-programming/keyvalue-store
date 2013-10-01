package distributed_group_mem;

import java.util.*;

public class MemberList {
	
	ArrayList<MemberListEntry> memList = new ArrayList<MemberListEntry>();
	
	MemberList (String localMachineID)
	{
		memList.add(new MemberListEntry (localMachineID));
		
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
	
	
	
	

}


