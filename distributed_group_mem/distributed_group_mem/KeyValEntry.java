package distributed_group_mem;

public class KeyValEntry {
	int identifier;
	Value val;
	
	KeyValEntry(int key, Value val)
	{
		identifier = key;
		this.val = val;
	}
	
	void print()
	{
		System.out.println(String.valueOf(identifier) + ":" + val.getinline());
	}
	
}
