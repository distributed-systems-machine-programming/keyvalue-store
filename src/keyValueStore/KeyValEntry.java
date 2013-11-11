package keyValueStore;

import java.io.Serializable;

public class KeyValEntry implements Serializable {
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
