package keyValueStore;

import java.io.Serializable;

public class Value implements Serializable {
	int ID;
	String name;
	
	Value(int id, String name)
	{
		ID = id;
		this.name = name;
	}

	public String getinline() {
		return String.valueOf(ID) + " " + name;
	}
}
