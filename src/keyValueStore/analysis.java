package keyValueStore;

import java.io.*;
import java.util.ArrayList;

public class analysis {
	ArrayList<Long> data;
	
	analysis()
	{
		data = new ArrayList<Long>();
	}
	
	void pushtofile() throws IOException
	{
		File file = new File("add_data.txt");
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		
	
		for (int i=0; i<data.size();i++)
		{
			bw.write(String.valueOf(data.get(i)));
			bw.write("\n");
		}
		bw.close();
	}
	
	
}
