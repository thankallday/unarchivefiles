package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class ConfigPropertyValues
{

  private static ILog logger = null;

  private static final int EXPIRATION_TIME = 1; // minutes

  private static Properties prop = new Properties();

  private static Date loadTime = null;

  private static String propFileName = "unarchivefiles.properties";

  public ConfigPropertyValues()
  {
    if (logger == null)
    {
      logger = new LogTrackerImpl(this.getClass().getName());
    }  
  }
  
  
  private static boolean isPropertiesExpired()
  {
    boolean expired = true;

    if (loadTime != null)
    {
      Date currentTime = new Date(System.currentTimeMillis());

      long diff = currentTime.getTime() - loadTime.getTime();
      long diffMinutes = diff / (60 * 1000) % 60;
      if (diffMinutes > EXPIRATION_TIME)
      {
        expired = true;
      }
      else
      {
        expired = false;
      }
    }

    return expired;
  }

  private static Properties getPropValues(String propertiesFileName) throws IOException
  {
    if (isPropertiesExpired())
    {
      prop.load(new FileInputStream((propertiesFileName == null ? propFileName : propertiesFileName)));
      loadTime = new Date(System.currentTimeMillis());
    }

    return prop;
  }

  public static String getProperty(String name)
  {
    String value = "";
    try
    {
      value = getPropValues(null).getProperty(name);
    }
    catch (IOException e)
    {
      logger.log(LogType.ERROR, "Failed to read environment - " + name + ". " + e.getMessage());
    }
    
    if ( value != null ) value.trim();
    return value;
  }

  public static String getProperty(String name, String propertiesFileName) throws IOException
  {
    return getPropValues(propertiesFileName).getProperty(name).trim();
  }
}