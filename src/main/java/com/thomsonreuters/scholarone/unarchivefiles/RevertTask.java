package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import com.amazonaws.services.s3.model.StorageClass;
import com.scholarone.activitytracker.IHeader;
import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.IMonitor;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;
import com.scholarone.activitytracker.ref.MonitorTrackerImpl;
import com.scholarone.archivefiles.common.S3File;
import com.scholarone.archivefiles.common.S3FileUtil;
import com.scholarone.monitoring.common.Environment;
import com.scholarone.monitoring.common.IMetricSubTypeConstants;
import com.scholarone.monitoring.common.MetricProduct;
import com.scholarone.monitoring.common.PublishMetrics;
import com.scholarone.monitoring.common.ServiceComponent;
import com.thomsonreuters.scholarone.unarchivefiles.audit.AuditFileRevert;
import com.thomsonreuters.scholarone.unarchivefiles.audit.S3FileAuditUtility;

public class RevertTask implements ITask
{
  private String environment;
  
  private Integer stackId;
  
  private Long runId;

  private Config config;

  private Document document;

  private ILog logger = null;

  private static IMonitor monitor = null;

  private S3File sourceS3Dir;

  private S3File destinationS3Dir;

  private String unarchiveCacheDir;

  private ILock lockObject;

  private int auditLevel;
  
  private int exitCode = -1;
  
  private Long transferTime;
  
  private Long transferSize;
  
  private Double transferRate;

  private String prefixDirectory;

  private Environment envType;

  private String envName;
  
  public RevertTask(Integer stackId, Config config, Document document, Long runId, int level, String environment, String unarchiveCacheDir, 
      String sourceBucketName, String destinationBucketName, String prefixDirectory, Environment envType, String envName) throws IOException
  {
    this.environment = ConfigPropertyValues.getProperty("environment");
    this.stackId = stackId;
    this.runId = runId;
    this.config = config;
    this.document = document;
    this.auditLevel = level;
    this.unarchiveCacheDir = unarchiveCacheDir;
    this.prefixDirectory = prefixDirectory;
    this.envType = envType;
    this.envName = envName;

    logger = new LogTrackerImpl(this.getClass().getName());
    ((IHeader)logger).addLocalHeader("ConfigId", config.getConfigId().toString());
    ((IHeader)logger).addLocalHeader("DocumentId", document.getDocumentId().toString());

    monitor = MonitorTrackerImpl.getInstance();

    sourceS3Dir = new S3File(prefixDirectory + File.separator + document.getArchiveYear()
        + File.separator + document.getArchiveMonth() + File.separator + environment + stackId 
        + File.separator + config.getShortName() + File.separator + document.getDocumentId() + File.separator, 
        sourceBucketName);
    
    destinationS3Dir = new S3File(prefixDirectory + File.separator
        + environment + stackId + File.separator + config.getShortName()
        + File.separator + document.getFileStoreYear() + File.separator + document.getFileStoreMonth() + File.separator
        + document.getDocumentId() + File.separator,
        destinationBucketName);

    lockObject = new TaskLock(this, unarchiveCacheDir);
  }

  public Config getConfig()
  {
    return config;
  }

  public Document getDocument()
  {
    return document;
  }

  public S3File getSourceS3Dir()
  {
    return sourceS3Dir;
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
    logger.log(LogType.INFO, "START RevertTask document. | " + sourceS3Dir.getKey() + " | " + destinationS3Dir.getKey());
    
    
    PublishMetrics.incrementTotalCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_DOCUMEMNT, envType, envName);

    StatInfo stat = new StatInfo();
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
    stat.setTransferRate(0.0);
    stat.setTransferSize(0L);
    stat.setTransferTime(0L);
    
    try
    {       
      //S3File dir = new S3File(sourceS3Dir.getKey(), sourceS3Dir.getBucketName());
      
      if (!S3FileUtil.isDirectory(sourceS3Dir))
      {
        logger.log(LogType.ERROR, "Directory does not exists, aborting. " + sourceS3Dir.getKey());
        //exitCode = -1;
        //PublishMetrics.incrementFailureCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_DOCUMEMNT, envType, envName);
        return;
      }
    
      if (lockObject.lock())
      {
        logger.log(LogType.INFO, sourceS3Dir.getKey() + " locked to move to " + destinationS3Dir.getKey());

        exitCode = S3FileUtil.copyS3Dir(sourceS3Dir, destinationS3Dir, null, StorageClass.Standard, stat);
        
        if (exitCode == 0)
        {
          transferSize = stat.getTransferSize();
          transferTime = stat.getTransferTime();
          transferRate = stat.getTransferRate();
          
          if(auditLevel == 1)
          {
            AuditFileRevert auditFileRevert = new AuditFileRevert(S3FileAuditUtility.getFilesAndPathsAsMap(sourceS3Dir), S3FileAuditUtility.getFilesAndPathsAsMap(destinationS3Dir));
            if(auditFileRevert.areFilesReverted())
            {
              exitCode = 0;
            }
            else
            {
              //exitCode = -1;
              logger.log(LogType.ERROR, sourceS3Dir.getKey() + " failed to audit with " + destinationS3Dir.getKey());
            }
          }
          else
          {
            exitCode = 0;
          }
        }
        else
        {
          //exitCode = -1;
          logger.log(LogType.ERROR, sourceS3Dir.getKey() + " failed to copy directory to " + destinationS3Dir.getKey());
        }
      }
      else
      {
        //exitCode = -1;
        logger.log(LogType.ERROR, "Failed to get lock. " + sourceS3Dir.getKey() + TaskLock.LOCK + " exists");
      }
    }
    catch (Exception e)
    {
      //exitCode = -1;
      logger.log(LogType.ERROR, sourceS3Dir.getKey() + " occurred exception " + e.getMessage());
    }
    finally
    {
      if ( exitCode == 0 )
      {
        stat.incrementSuccessCount();
        updateDB(true);
        PublishMetrics.incrementSuccessCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_DOCUMEMNT, envType, envName);
        PublishMetrics.logRate(MetricProduct.S1M, stat.getTransferRate(), ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_DOCUMEMNT, envType, envName);
      }
      else
      {
        S3FileUtil.removeFile(getSourceS3Dir().getKey() + TaskLock.LOCK, getSourceS3Dir().getBucketName());
        stat.incrementFailureCount();
        document.incrementRetryCount();
        updateDB(false);
        PublishMetrics.incrementFailureCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_DOCUMEMNT, envType, envName);
      }
      
      lockObject.unlock();
      logger.log(LogType.INFO, sourceS3Dir.getKey() + " unlocked");
      
      stat.markEndTime();

      DecimalFormat formatter = new DecimalFormat("#0");
      StringBuilder sb = new StringBuilder();
      sb.append("name=" + stat.getName())
            .append(", type=" + stat.getType())
            .append(", message=" + stat.getMessage())
            .append(", environment=" + stat.getEnvironment())
            .append(", stackId=" + stackId)
            .append(", groupId=" + stat.getGroupId())     
            .append(", documentId=" + document.getDocumentId().longValue())
            .append(", transferSize=" + stat.getTransferSize() + " bytes")
            .append(", transferTime=" + stat.getTransferTime() + " ms")
            .append(", transferRate=" + (stat.getTransferRate() == null ? "" : formatter.format(stat.getTransferRate()) + " bytes/sec"))
            .append(", totalCount=" + stat.getTotalCount())
            .append(", successCount=" + stat.getSuccessCount())
            .append(", failureCount=" + stat.getFailureCount())
            .append(", numOfFilesTotal=" + stat.getNumOfFilesTotal())
            .append(", numberOfFilesSuccess=" + stat.getNumOfFilesSuccess())
            .append(", numberOfFilesFailure=" + stat.getNumOfFilesFailure())
            .append(", startTime=" + stat.getStartTime())
            .append(", endTime=" + stat.getEndTime())
            .append(", elapsedTime=" + (stat.getEndTime().getTime() - stat.getStartTime().getTime()) + " ms");
      
      logger.log(LogType.INFO, sb.toString());
      if (exitCode == 0)
        logger.log(LogType.INFO, "END RevertTask document. SUCCESS. | " + sourceS3Dir.getKey() + " | " + destinationS3Dir.getKey());
      else
        logger.log(LogType.ERROR, "END RevertTask document. FAILURE. | " + sourceS3Dir.getKey() + " | " + destinationS3Dir.getKey());
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
