package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class TaskFactoryImpl implements ITaskFactory
{
  private static ILog logger = null;

  private Integer stackId = null;
  
  private Long runId;

  public TaskFactoryImpl(Integer stackId, Long runId)
  {
    this.stackId = stackId;
    
    this.runId = runId;

    if (logger == null)
    {
      logger = new LogTrackerImpl(this.getClass().getName());
    }
  }

  @Override
  public List<ITask> getTasks()
  {
    Integer documentCount = Integer.valueOf(100);

    try
    {
      documentCount = Integer.valueOf(ConfigPropertyValues.getProperty("documents.per.config." + stackId));
    }
    catch (NumberFormatException | IOException e)
    {
      logger.log(LogType.WARN, "Failed to read documents.per.config." + stackId + ".  Using defaults. " + e.getMessage());
    }

    String configIdStr = null;

    try
    {
      configIdStr = ConfigPropertyValues.getProperty("configs." + stackId);
    }
    catch (NumberFormatException | IOException e)
    {
      logger.log(LogType.WARN, "Fail to read configs." + stackId + ".  Using defaults. " + e.getMessage());
    }

    Integer auditLevel = Integer.valueOf(ITask.AUDIT);

    try
    {
      auditLevel = Integer.valueOf(ConfigPropertyValues.getProperty("audit.level." + stackId));
    }
    catch (NumberFormatException | IOException e)
    {
      logger.log(LogType.WARN, "Fail to read audit.level." + stackId + ".  Using defaults. " + e.getMessage());
    }
    
    IUnarchiveFilesDAO db = new UnarchiveFilesDAOImpl();
    List<ITask> tasks = new ArrayList<ITask>();

    try
    {
      ArrayList<Integer> configIds = new ArrayList<Integer>();
      if ( configIdStr != null ) 
      {
        StringTokenizer st = new StringTokenizer(configIdStr, ",");
        while (st.hasMoreElements())
        {
          configIds.add(Integer.valueOf((String) st.nextElement()));
        }
      }
      
      if (db.openConnection(stackId))
      {
        List<Config> configs = db.getConfigs();
        for (Config config : configs)
        {
          if (configIds.isEmpty() || configIds.contains(config.getConfigId()))
          {
            List<Document> documents = db.getDocuments(config.getConfigId(), documentCount);
            if (documents.size() > 0)
            {
              for (Document document : documents)
              {
                try
                {
                  tasks.add(new RevertTask(stackId, config, document, runId, auditLevel.intValue()));
                }
                catch (Exception e)
                {
                  logger.log(LogType.ERROR, "ConfigId: " + config.getConfigId() + " - DocumentId: " + document.getDocumentId() + " - " + e.getMessage());
                }
              }
            }
          }
        }
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

    return tasks;
  }
}
