package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.IOException;

import com.scholarone.activitytracker.IHeader;
import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.IMonitor;
import com.scholarone.activitytracker.TrackingInfo;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;
import com.scholarone.activitytracker.ref.MonitorTrackerImpl;
import com.thomsonreuters.scholarone.unarchivefiles.audit.AuditFileRevert;
import com.thomsonreuters.scholarone.unarchivefiles.audit.FileAuditUtility;

public class RevertTask implements ITask
{
  private String environment;
  
  private Integer stackId;
  
  private Long runId;

  private Config config;

  private Document document;

  private ILog logger = null;

  private static IMonitor monitor = null;
  
  private String source;
  
  private String sourceDocfilesDir;

  private String destination;
  
  private String destinationDocfilesDir;

  private ILock lockObject;

  private int auditLevel;
  
  private int exitCode = -1;
  
  private Long transferTime;
  
  private Long transferSize;
  
  private Double transferRate;

  public RevertTask(Integer stackId, Config config, Document document, Long runId, int level) throws IOException
  {
    this.environment = ConfigPropertyValues.getProperty("environment");
    this.stackId = stackId;
    this.runId = runId;
    this.config = config;
    this.document = document;
    this.auditLevel = level;

    logger = new LogTrackerImpl(this.getClass().getName());
    ((IHeader)logger).addLocalHeader("ConfigId", config.getConfigId().toString());
    ((IHeader)logger).addLocalHeader("DocumentId", document.getDocumentId().toString());
    
    monitor = MonitorTrackerImpl.getInstance();
    
    source = ConfigPropertyValues.getProperty("tier3.directory") + File.separator + document.getArchiveYear()
        + File.separator + document.getArchiveMonth() + File.separator + ConfigPropertyValues.getProperty("environment") + stackId 
        + File.separator + config.getShortName() + File.separator + document.getDocumentId();
    
    sourceDocfilesDir = source + File.separator + "docfiles"; 
    
    destination = ConfigPropertyValues.getProperty("tier2.directory") + File.separator + 
        ConfigPropertyValues.getProperty("environment") + stackId + File.separator + 
        config.getShortName() + File.separator + 
        document.getFileStoreYear() + File.separator + 
        document.getFileStoreMonth() + File.separator + document.getDocumentId();

    destinationDocfilesDir = destination + File.separator + "docfiles"; 

    lockObject = new TaskLock(this);
  }

  public Config getConfig()
  {
    return config;
  }

  public Document getDocument()
  {
    return document;
  }

  public String getSource()
  {
    return source;
  }
  
  public String getSourceDocfilesDir()
  {
    return sourceDocfilesDir;
  }

  public Integer getStackId()
  {
    return stackId;
  }
  
  public int getExitCode()
  {
    return exitCode;
  }
  
  public Long getTransferTime()
  {
    return transferTime;
  }

  public Long getTransferSize()
  {
    return transferSize;
  }
  
  public Double getTransferRate()
  {
    return transferRate;
  }


  public void run()
  {
    logger.log(LogType.INFO, "Start Task");

    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(stackId);

    TrackingInfo stat = new TrackingInfo();
    stat.setType(MonitorConstants.FILE_UNARCHIVE_DOCUMENT);
    stat.setStackId(stackId);
    stat.setEnvironment("s1m-" + environment + "-stack" + stackId);
    stat.setGroupId(runId);
    stat.setObjectId(document.getDocumentId().longValue());
    stat.setObjectTypeId(ObjectTypeConstants.DOCUMENT_ID);
    stat.setMessage(((IHeader)monitor).getGlobalHeaders());
    stat.setName("Unarchive Document Information");
    stat.setTotalCount(1);
    stat.markStartTime();
    
    try
    {       
      File dir = new File(getSource());
      if ( !dir.exists() )
      {
        logger.log(LogType.INFO, "Directory does not exists, aborting lock - " + getSource());
        
        document.incrementRetryCount();
        stat.incrementFailureCount();
        exitCode = -1;
        
        return;
      }
    
      if (lockObject.lock())
      {
        logger.log(LogType.INFO, "Locked");

        exitCode = fs.copy(sourceDocfilesDir, destination, stat); //restore docfiles only, not lock files and exclude_from.txt.
        if (exitCode == 0)
        {
          transferSize = stat.getTransferSize();
          transferTime = stat.getTransferTime();
          transferRate = stat.getTransferRate();
          
          if(auditLevel == 1)
          {
            AuditFileRevert auditFileRevert = new AuditFileRevert(FileAuditUtility.getFilesAndPathsAsMap(sourceDocfilesDir), FileAuditUtility.getFilesAndPathsAsMap(destinationDocfilesDir));
            if(auditFileRevert.areFilesReverted())
              stat.incrementSuccessCount();
            else
            {
              exitCode = -1;
              document.incrementRetryCount();
              stat.incrementFailureCount();
            }
          }
          else
          {
            stat.incrementSuccessCount();
          }
        }
        else
        {
          document.incrementRetryCount();
          stat.incrementFailureCount();
        }
      }
      else
      {
        document.incrementRetryCount();
        stat.incrementFailureCount();
        logger.log(LogType.INFO, "Failed to get lock");
      }
    }
    catch (Exception e)
    {
      document.incrementRetryCount();
      stat.incrementFailureCount();
      logger.log(LogType.ERROR, e.getMessage());
    }
    finally
    {
      if ( exitCode == 0 )
        updateDB(true);
      else
        updateDB(false);
      
      lockObject.unlock();
      logger.log(LogType.INFO, "Unlock");
      
      stat.markEndTime();
      monitor.monitor(stat);
    }
  }

  private void updateDB(boolean filesCopySuccess)
  {
    IUnarchiveFilesDAO db = new UnarchiveFilesDAOImpl();

    try
    {
      if (db.openConnection(stackId))
      {
    	  db.updateDocument(document.getDocumentId(), document.getRetryCount(), filesCopySuccess);
      }
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }
    finally
    {
      db.closeConnection();
    }
  }
}
