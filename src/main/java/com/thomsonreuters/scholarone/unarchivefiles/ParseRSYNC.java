package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.TrackingInfo;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class ParseRSYNC
{
  public static String SENDING = "sending";
  public static String SENT = "sent";
  public static String TOTAL = "total";
  public static String PERCENT = "%";
  
  public static void parse(InputStream in, TrackingInfo stat, Integer stackId)
  {
    ILog logger = new LogTrackerImpl(ParseRSYNC.class.getName());
    
    try
    {
      BufferedReader d = new BufferedReader(new InputStreamReader(in));
      String line = "";
      while ((line = d.readLine()) != null)
      {
        if ( line.contains(SENDING) || line.contains(PERCENT) ) continue;
        
        if ( line.contains(SENT) )
        {
          String[] tokens = line.split("[ ]+");
          stat.setTransferRate(new Double(tokens[6]));
        }
        else if ( line.contains(TOTAL))
        {
          String[] tokens = line.split("[ ]+");
          stat.setTransferSize(new Double(tokens[3]).longValue());
        }
        else
        {
          if ( line.trim().length() > 0 )
            logger.log(LogType.INFO, "Transferred file - " + line);
        }
      }
            
      d.close();
      
      if ( stat.getTransferRate() != null && stat.getTransferSize() != null )
      {
        Double transferTime = ( stat.getTransferSize() == 0 ? 0 :  stat.getTransferSize() / stat.getTransferRate()) * 1000; // Convert from seconds to milliseconds
        stat.setTransferTime(transferTime.longValue());
      }
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }
  }
}
