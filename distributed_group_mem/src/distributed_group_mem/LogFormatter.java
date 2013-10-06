

package distributed_group_mem;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

//THIS CLASS DEFINES THE FORMAT OF EACH LOG ENTRY

class LogFormatter extends Formatter {
  
  public String format(LogRecord rec) {
    StringBuffer buf = new StringBuffer(1000);
    buf.append(rec.getLevel());
    
    buf.append(" : ");
    buf.append(calcDate(rec.getMillis()));
    buf.append(" # ");
    buf.append(formatMessage(rec));
    buf.append('\n');
    return buf.toString();
  }

  private String calcDate(long millisecs) {
    SimpleDateFormat date_format = new SimpleDateFormat("MMM/dd/yyyy HH.mm.ss.SSS");
    Date resultdate = new Date(millisecs);
    return date_format.format(resultdate);
  }

  
} 
 
