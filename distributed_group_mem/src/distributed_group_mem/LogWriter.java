package distributed_group_mem;

import java.io.*;
import java.util.logging.*;


public class LogWriter {
  static private FileHandler fileTxt;
  static private Formatter formatterTxt;

  static public void setup(String MachineName) throws IOException {

    // Get the global logger to configure it
    Logger logger = Logger.getLogger("");

    logger.setLevel(Level.INFO);
    String logFile = MachineName+".log";
    fileTxt = new FileHandler(logFile);
    

    // Create txt Formatter
    //formatterTxt = new SimpleFormatter();
    //fileTxt.setFormatter(formatterTxt);
    //logger.addHandler(fileTxt);
    formatterTxt = new LogFormatter();
    fileTxt.setFormatter(formatterTxt);
    logger.addHandler(fileTxt);


 
  }
}