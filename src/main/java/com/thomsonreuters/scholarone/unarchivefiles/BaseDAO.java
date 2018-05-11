package com.thomsonreuters.scholarone.unarchivefiles;

import java.sql.Connection;
import java.sql.DriverManager;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class BaseDAO
{
  protected Connection connection = null;
  protected ILog logger = new LogTrackerImpl(this.getClass().getName());
  protected Integer stackId;
  
  public boolean openConnection(Integer stackId)
  {
    boolean open = false;
    
    this.stackId = stackId;

    try
    {
      String jdbcClassName = ConfigPropertyValues.getProperty("jdbc.class.name");
      String url = ConfigPropertyValues.getProperty("database.url." + stackId);
      String user = ConfigPropertyValues.getProperty("database.user");
      String password = ConfigPropertyValues.getProperty("database.password");

      // Load class into memory
      Class.forName(jdbcClassName);

      // Establish connection
      connection = DriverManager.getConnection(url, user, password);
      open = true;
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }

    return open;
  }

  public void closeConnection()
  {
    if (connection != null)
    {
      try
      {
        connection.close();
      }
      catch (Exception e)
      {
        logger.log(LogType.ERROR, e.getMessage());
      }
      finally
      {
        connection = null;
      }
    }
  }
}
