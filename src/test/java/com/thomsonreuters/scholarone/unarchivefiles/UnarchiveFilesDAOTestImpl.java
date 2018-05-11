package com.thomsonreuters.scholarone.unarchivefiles;

import java.sql.SQLException;
import java.sql.Statement;

import com.scholarone.activitytracker.ref.LogType;
import com.thomsonreuters.scholarone.unarchivefiles.UnarchiveFilesDAOImpl;

public class UnarchiveFilesDAOTestImpl extends UnarchiveFilesDAOImpl
{

  public boolean addDocument()
  {
    boolean success = false;

    String insertSQL = "insert into document (document_id, config_id, archive_status_id, archive_date, unarchive_move_retry_count, datetime_moved_tier3, datetime_added, added_by) values (0, 534, 24, current timestamp + 1 day, 0, current timestamp - 1 day, current timestamp, -3141593)";
    
    Statement statement = null;
    if (connection != null)
    {
      try
      {
        statement = connection.createStatement();
        int r = statement.executeUpdate(insertSQL);
        if ( r == 1 )
          success = true;
      }
      catch (SQLException e)
      {
        logger.log(LogType.ERROR, e.getMessage());
      }
      finally
      {
        if (statement != null)
        {
          try
          {
            statement.close();
          }
          catch (SQLException e)
          {
            logger.log(LogType.ERROR, e.getMessage());
          }
        }
      }
    }

    return success;
  }
  
  public boolean deleteDocument()
  {
    boolean success = false;

    String deleteSQL = "delete from document where document_id = 0";
    
    Statement statement = null;
    if (connection != null)
    {
      try
      {
        statement = connection.createStatement();
        int r = statement.executeUpdate(deleteSQL);
        if ( r == 1 )
          success = true;
      }
      catch (SQLException e)
      {
        logger.log(LogType.ERROR, e.getMessage());
      }
      finally
      {
        if (statement != null)
        {
          try
          {
            statement.close();
          }
          catch (SQLException e)
          {
            logger.log(LogType.ERROR, e.getMessage());
          }
        }
      }
    }

    return success;
  }
}
