package distributed_group_mem;

import java.io.*;
import java.util.logging.*;


public class LogWriter {
  static private FileHandler fileTxt;
  static private Formatter formatterTxt;

  static public void setup(String MachineName) throws IOException {

    // Get the global logger to configure it
    Logger logger = Logger.getLogger("");
    
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