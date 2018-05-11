package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.scholarone.activitytracker.IHeader;
import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.IMonitor;
import com.scholarone.activitytracker.TrackingInfo;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;
import com.scholarone.activitytracker.ref.MonitorTrackerImpl;

public class TaskProcessor extends Thread
{
  private ILog logger = null;

  private IMonitor monitorTracker = null;
  
  private Integer stackId;

  private ITaskFactory factory = null;

  private long completedCount;

  private List<ITask> tasks;

  private Boolean stop = new Boolean(false);
  
  private long runId;

  public TaskProcessor(Integer stackId, Long runId, ITaskFactory factory)
  {
    this.factory = factory;
    this.stackId = stackId;
    this.runId = runId;
    
    logger = new LogTrackerImpl(this.getClass().getName());
    ((IHeader)logger).clearGlobalHeaders();
    
    ((IHeader)logger).addGlobalHeader("StackId", stackId.toString());
    ((IHeader)logger).addGlobalHeader("RunId", Long.valueOf(runId).toString());
    
    monitorTracker = MonitorTrackerImpl.getInstance();
  }

  public void setTasks(List<ITask> tasks)
  {
    this.tasks = tasks;
  }

  public void run()
  {
    if (factory == null)
    {
      logger.log(LogType.ERROR, "ITaskFactory was not supplied.");
      return;
    }

    if (stackId == null)
    {
      logger.log(LogType.ERROR, "StackId was not supplied");
      return;
    }

    String environment = "dev";
    try
    {
      environment = ConfigPropertyValues.getProperty("environment");
    }
    catch (NumberFormatException | IOException e)
    {
      logger.log(LogType.WARN, "Failed to read environment. Using default. - " + e.getMessage());
    }
    
    Integer poolSize = Integer.valueOf(5);

    try
    {
      poolSize = Integer.valueOf(ConfigPropertyValues.getProperty("threadpool.size." + stackId));
    }
    catch (NumberFormatException | IOException e)
    {
      logger.log(LogType.WARN, "Failed to read threadpool.size. " + stackId + ".  Using default. - " + e.getMessage());
    }
   
    logger.log(LogType.INFO, "begin unarchiving job ===============================================");
    
    String lockFilePath = "lock" + File.separator + ".lock-stack" + stackId;
    File lockFileDir = new File("lock");
    File lockFile = new File(lockFilePath);
    try
    {
      if (!lockFileDir.exists()) lockFileDir.mkdirs();     
      if (lockFile.exists())
      {
        logger.log(LogType.INFO, "abort unarchiving job becasue previous job is still running.");
        return;
      }
      else
      {
        FileOutputStream out = new FileOutputStream(lockFilePath);
        PrintStream p = new PrintStream(out);
        p.close();
      }
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, "abort unarchiving job because of error when checking/creating lock file.");
      logger.log(LogType.ERROR, e.getMessage());
      return;
    }
    logger.log(LogType.INFO, "lock file " + lockFile.getName() + " created.");    
       
    if (tasks == null) tasks = factory.getTasks();
   
    TrackingInfo stat = new TrackingInfo();
    stat.setType(MonitorConstants.FILE_UNARCHIVE_RUN);
    stat.setEnvironment("s1m-" + environment + "-stack" + stackId);
    stat.setStackId(stackId);
    stat.setMessage(((IHeader)monitorTracker).getGlobalHeaders());
    stat.markStartTime();
    stat.setTotalCount(tasks.size());
    stat.setGroupId(runId);
    
    ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);
    
    try
    {
      logger.log(LogType.INFO, "Processing " + tasks.size() + " tasks");

      for (ITask task : tasks)
      {
        pool.execute(task);
      }

      while (pool.getActiveCount() > 0)
      {
        try
        {
          String stopValue = ConfigPropertyValues.getProperty("stop." + stackId);
          if (stopValue != null) setStop(true);
        }
        catch (NumberFormatException | IOException e)
        {
          logger.log(LogType.WARN, "Fail to read stop. " + stackId + ".  Using default - " + e.getMessage());
        }

        if (isStop())
        {
          pool.shutdown();
          pool.awaitTermination(10, TimeUnit.SECONDS);
          setStop(false);
          logger.log(LogType.INFO, "TaskProcessor shutdown.");
          break;
        }

        completedCount = pool.getCompletedTaskCount();

        try
        {
          poolSize = Integer.valueOf(ConfigPropertyValues.getProperty("threadpool.size." + stackId));
        }
        catch (NumberFormatException | IOException e)
        {
          logger.log(LogType.WARN, "Fail to read property - threadpool.size." + stackId + " - " + e.getMessage());
        }

        if (pool.getCorePoolSize() != poolSize.intValue())
        {
          pool.setCorePoolSize(poolSize);
          logger.log(LogType.INFO, "Changed pool size to " + pool.getCorePoolSize());
        }

        Thread.sleep(1000);
      }

      completedCount = pool.getCompletedTaskCount();
      logger.log(LogType.INFO, "Completed " + completedCount + " tasks");
      
      stat.markEndTime();
      stat.setTotalCount((int)completedCount);
      
      long transferTime = 0, transferSize = 0;
      double totalTransferRate = 0;
      for (ITask task : tasks)
      {
        if ( task.getExitCode() == 0 )
          stat.incrementSuccessCount();
        else
          stat.incrementFailureCount();
        
        if ( task.getTransferTime() != null )
          transferTime += task.getTransferTime();
        if ( task.getTransferSize() != null )
          transferSize += task.getTransferSize();
        if ( task.getTransferRate() != null )
          totalTransferRate += task.getTransferRate();        
      }
            
      stat.setTransferSize(transferSize);
      stat.setTransferTime(transferTime);
      stat.setTransferRate(totalTransferRate/tasks.size());
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }
    finally
    {
      pool.shutdown();
      
      stat.setName("Unarchive Run Information");
      monitorTracker.monitor(stat);
    }
    
    try
    {
      if (lockFile.exists())
      {
        lockFile.delete();
        logger.log(LogType.INFO, lockFile.getName() + " deleted.");
      }
    }
    catch (Exception e)
    {
      logger.log(LogType.ERROR, "lock file " + lockFile.getName() + " deletion failed.");
      e.printStackTrace();
    }
    logger.log(LogType.INFO, "end unarchiving job =================================================");
  }

  public long getCompletedCount()
  {
    return completedCount;
  }

  public boolean isStop()
  {
    synchronized (this.stop)
    {
      return stop;
    }
  }

  public void setStop(boolean stop)
  {
    synchronized (this.stop)
    {
      this.stop = stop;
    }
  }
}
