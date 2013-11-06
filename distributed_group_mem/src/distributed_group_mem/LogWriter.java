package distributed_group_mem;

import java.io.*;
import java.util.logging.*;


//THIS CLASS INITIALIZES THE LOG WRITER WITH LOG LEVEL PARAMETER AND ASSIGNS THE OUTPUT FILE NAME

public class LogWriter {
  static private FileHandler fileTxt;
  static private Formatter formatterTxt;

  static public void setup(String MachineName, String LoggingLevel) throws IOException {

    // Get the global logger to configure it
    Logger logger = Logger.getLogger("");
    
    if(LoggingLevel.equalsIgnoreCase("FINE"))
    	logger.setLevel(Level.FINE);
    else if(LoggingLevel.equalsIgnoreCase("INFO"))
    	logger.setLevel(Level.INFO);
    else if(LoggingLevel.equalsIgnoreCase("FINER"))
    	logger.setLevel(Level.FINER);
    else if(LoggingLevel.equalsIgnoreCase("WARNING"))
    	logger.setLevel(Level.WARNING);
    else
    	logger.setLevel(Level.FINE);
    
    String logFile = MachineName+".log";
    fileTxt = new FileHandler(logFile);
    
    Handler[] allHandles = logger.getHandlers();
    for (int i=0; i<allHandles.length; i++)
    	logger.removeHandler(allHandles[i]);

    // Create txt Formatter
    //formatterTxt = new SimpleFormatter();
    //fileTxt.setFormatter(formatterTxt);
    //logger.addHandler(fileTxt);
    formatterTxt = new LogFormatter();
    fileTxt.setFormatter(formatterTxt);
    logger.addHandler(fileTxt);


 
  }
}