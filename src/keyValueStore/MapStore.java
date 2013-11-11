package keyValueStore;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MapStore {
	private TreeMap<Integer, Value> x = null;
	MapStore(String xmlFile)	//this is for testing
	{
		x = new TreeMap<Integer, Value>();
		try {
			 
			File fXmlFile = new File(xmlFile);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
		 
			doc.getDocumentElement().normalize();
		 
			//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
		// System.out.println ("All values are in Milliseconds");
			NodeList nList = doc.getElementsByTagName("key");
		 
		
		 
			for (int temp = 0; temp < nList.getLength(); temp++) {
		 
				Node nNode = nList.item(temp);
		 			 
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
		 
					Element eElement = (Element) nNode;
		 
					
					addToMapStore(eElement.getAttribute("id"),eElement.getElementsByTagName("ID").item(0).getTextContent(),eElement.getElementsByTagName("name").item(0).getTextContent());
								 
				}
			}
		    } catch (Exception e) {
			e.printStackTrace();
		    }
	}

	

	MapStore()
	{
		x =new TreeMap<Integer, Value>();
		 
	}
	
	
	   private void addToMapStore(String attribute, String id, String name) {
           int attr = Integer.parseInt(attribute);
           Value temp = new Value(Integer.parseInt(id), name);
           x.put(attr, temp);
           
   }
   // ADD, DELETE AND UPDATE return true if the respective actions worked, false otherwise.
   public Boolean addEntry(int key, Value val)
   {                
           if(x.containsKey(key) == false) {
                   x.put(key, val);
                   return true;
           }
           else
                   return false;
   }
   public Boolean deleteEntry(int key)
   {
           if(x.containsKey(key)){
                   x.remove(key);
                   return true;
           }
           else
                   return false;
   }
   public Value lookupEntry(int key)
   {
           Value val = null;
           if (x.containsKey(key))                
                   val = x.get(key);
           
           return val;        
           
   }
   public Boolean updateEntry(int key, Value newval)
   {
           if(x.containsKey(key)) {
                   x.put(key, newval);
                   return true;
           }
           else 
                   return false;
           
   }
   public void printMapStore()
   {
           for (Entry<Integer, Value> entry : x.entrySet()) {
                   System.out.println(entry.getKey() + "        " + entry.getValue().ID + " " + entry.getValue().name);
           }                
   }
	
	NavigableMap<Integer,Value> getKeys(int identifier)
	{
		return x.headMap(identifier, true);
	}
	
	Map<Integer, Value> getKeys()
	{
		return x;
	}



	public void count() {
		System.out.println("Count:" + x.size());
		
	}
	
	
	
}
