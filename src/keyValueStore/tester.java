package keyValueStore;

import java.util.Map;
import java.util.Map.Entry;

public class tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MapStore localMap = new MapStore("sampleKeyValStore.xml");
		
		
		localMap.printMapStore();
		localMap.addEntry(861898, new Value(988, "Facebook"));
		localMap.printMapStore();
		printMapStore(localMap.getKeys(860000));
		localMap.deleteEntry(45);
		localMap.updateEntry(1211144, new Value(99, "Twitter"));
		localMap.printMapStore();
	}
	
	public static void printMapStore(Map<Integer,Value> map)
	   {
	           for (Entry<Integer, Value> entry : map.entrySet()) {
	                   System.out.println(entry.getKey() + "        " + entry.getValue().ID + " " + entry.getValue().name);
	           }                
	   }

}
