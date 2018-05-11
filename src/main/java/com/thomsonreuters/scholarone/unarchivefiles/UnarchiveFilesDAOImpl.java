package com.thomsonreuters.scholarone.unarchivefiles;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import com.scholarone.activitytracker.ref.LogType;

public class UnarchiveFilesDAOImpl extends BaseDAO implements IUnarchiveFilesDAO
{
  /*
   * (non-Javadoc)
   * 
   * @see com.thomsonreuters.scholarone.cand.IUnarchivesFilesDAO#getConfigs()
   */
  @Override
  public List<Config> getConfigs() throws SQLException
  {
    List<Config> configs = new ArrayList<Config>();
    HashMap<Integer, Config> configMap = new HashMap<Integer, Config>();

    PreparedStatement statement = null;
    if (connection != null)
    {
      try
      {
        String query = "select c.config_id, o.short_name from organization o, config c where c.organization_id = o.organization_id for fetch only";

        statement = connection.prepareStatement(query);

        ResultSet rs = statement.executeQuery();
        while (rs.next())
        {
          Config config = new Config();
          config.setConfigId(rs.getInt("CONFIG_ID"));
          config.setShortName(rs.getString("SHORT_NAME"));

          configs.add(config);
          configMap.put(config.getConfigId(), config);
        }

      }
      catch (SQLException e)
      {
        logger.log(LogType.ERROR, e.getMessage()); 
        throw e;
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
    else
    {
      throw new SQLException("There is no database connection");
    }

    return configs;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.thomsonreuters.scholarone.cand.IArchivesFilesDAO#getDocuments(java.lang.Integer)
   */
  @Override
  public List<Document> getDocuments(Integer configId, Integer rows) throws SQLException
  {
    List<Document> documents = new ArrayList<Document>();

    PreparedStatement statement = null;
    if (connection != null)
    {
      try
      {
        String query = "select t.DOCUMENT_ID, t.unarchive_move_retry_count, t.archive_date, t.datetime_archive_restore_req, t.file_store_year, t.file_store_month, t.datetime_moved_tier3 "
            + "from ( "
            + "select "
            + "    ROWNUMBER() OVER() rownum, "
            + "    d.DOCUMENT_ID, "
            + "    d.unarchive_move_retry_count, "
            + "    d.archive_date, "
            + "    d.datetime_archive_restore_req, "
            + "    d.file_store_year, "
            + "    d.file_store_month, "
            + "    d.datetime_moved_tier3 "
            + "from document d "
            + "where "
            + "    d.config_id = ? and "
            + "    d.archive_status_id = 24 and "
            + "    d.datetime_moved_tier3 is not null and "
            + "    d.datetime_archive_restore_req is not null and "
            + "    d.datetime_deleted is null and "
            + "    (d.unarchive_move_retry_count is null or d.unarchive_move_retry_count < 5) "
            + ") t where t.rownum <= ? ";
        
        statement = connection.prepareStatement(query);

        statement.setInt(1, configId);
        statement.setInt(2, rows);

        ResultSet rs = statement.executeQuery();

        while (rs.next())
        {
          Document document = new Document();
          document.setDocumentId(rs.getInt("DOCUMENT_ID"));
          document.setRetryCount((int) rs.getShort("UNARCHIVE_MOVE_RETRY_COUNT"));
          document.setFileStoreMonth(rs.getInt("FILE_STORE_MONTH"));
          document.setFileStoreYear(rs.getInt("FILE_STORE_YEAR"));
          document.setDatetimeFilesMoved(rs.getTimestamp("DATETIME_MOVED_TIER3"));

          Timestamp archiveRestoreReqDate = rs.getTimestamp("DATETIME_ARCHIVE_RESTORE_REQ");
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(archiveRestoreReqDate.getTime());
          document.setArchiveMonth(cal.get(Calendar.MONTH) + 1);
          document.setArchiveYear(cal.get(Calendar.YEAR));
          documents.add(document);
          
          logger.log(LogType.INFO, "Include config [" + configId + "], document [" + document.getDocumentId() + "] for processing");
        }
      }
      catch (SQLException e)
      {
        logger.log(LogType.ERROR, "ConfigId: " + configId + " - " + e.getMessage());
        throw e;
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
            logger.log(LogType.ERROR, "ConfigId: " + configId + " - " + e.getMessage());
          }
        }
      }
    }
    else
    {
      throw new SQLException("There is no database connection");
    }

    return documents;
  }

  public boolean updateDocument(Integer documentId, Integer retryCount, boolean filesCopySuccess) throws SQLException
  {
    boolean updateSuccess = false;

    PreparedStatement statement = null;
    if (connection != null)
    {
      try
      {
    	  String sql = null;
    	  if (filesCopySuccess)
    	  {
    		  sql = "UPDATE DOCUMENT SET "
					+ "DATETIME_MOVED_TIER3 = null, "
					+ "UNARCHIVE_MOVE_RETRY_COUNT = ?, "
					+ "MOVE_RETRY_COUNT = 0, "
					+ "ARCHIVE_RETRY_NO = 0, "
					+ "ARCHIVE_PDF_PROOF_RETRY_NO = 0, "
					+ "RENDITION_PDF_PROOF_RETRY_NO = 0, "
					+ "MODIFIED_BY = 0 "
					+ "WHERE DOCUMENT_ID = ?";
    	  }
    	  else
    	  {
    		  sql = "UPDATE DOCUMENT SET "
  		            + "UNARCHIVE_MOVE_RETRY_COUNT = ?, "
  		            + "MODIFIED_BY = 0 "
  		            + "WHERE DOCUMENT_ID = ?";
    	  }
    	  
		  statement = connection.prepareStatement(sql);
		  if ( retryCount != null )
			  statement.setShort(1, retryCount.shortValue());
		  else
			  statement.setShort(1, (short) 0);
		  statement.setInt(2, documentId);

        statement.executeUpdate();
        updateSuccess = true;
      }
      catch (SQLException e)
      {
        logger.log(LogType.ERROR, "DocumentId: " + documentId + " - " + e.getMessage());
        throw e;
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
            logger.log(LogType.ERROR, "DocumentId: " + documentId + " - " + e.getMessage());
          }
        }
      }
    }
    else
    {
      throw new SQLException("There is no database connection");
    }

    return updateSuccess;
  }
}
