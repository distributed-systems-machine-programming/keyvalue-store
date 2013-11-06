package distributed_group_mem;

import java.io.File;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MapStore {
	Map<Integer, Value> x = new HashMap<Integer, Value>();
	
	MapStore(String xmlFile)	//this is for testing
	{
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

	private void addToMapStore(String attribute, String id, String name) {
		int attr = Integer.parseInt(attribute);
		Value temp = new Value(Integer.parseInt(id), name);
		x.put(attr, temp);
		
	}
	
	void addToMapStore(int key, Value val)
	{
		
	}
}
