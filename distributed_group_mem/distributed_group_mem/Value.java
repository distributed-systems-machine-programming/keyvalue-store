package distributed_group_mem;

public class Value {
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
