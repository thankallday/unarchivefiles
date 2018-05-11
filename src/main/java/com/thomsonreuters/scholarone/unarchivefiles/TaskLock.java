package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class TaskLock implements ILock
{
  public static final String LOCK = "tier3unarchive.lock";

  private static ILog logger = null;

  private boolean isLocked = false;

  private RevertTask task = null;

  private String processId = null;

  public TaskLock(RevertTask task)
  {
    this.task = task;
    processId = Runtime.getRuntime().toString();

    if (logger == null)
    {
      logger = new LogTrackerImpl(this.getClass().getName());
    }
  }

  @Override
  public boolean lock()
  {
    PrintWriter out = null;

    try
    {   
      if (checkForLock())
      {
        File lockFile = new File(task.getSource(), LOCK);
        out = new PrintWriter(lockFile);
        out.println(processId);
        out.println(new Date().getTime());
        out.close();
      }

      Thread.sleep(500);

      if (checkForLock())
        isLocked = true;
      else
        isLocked = false;

    }
    catch (Exception e)
    {
      isLocked = false;

      logger.log(LogType.ERROR, "ConfigId: " + task.getConfig().getConfigId() + " - DocumentId: " + task.getDocument().getDocumentId() + " - " + e.getMessage());
    }
    finally
    {
      if (out != null) out.close();
    }

    return isLocked;
  }

  @Override
  public void unlock()
  {
    if (isLocked)
    {
      File lockFile = new File(task.getSource(), LOCK);
      lockFile.delete();
    }
  }

  private boolean checkForLock()
  {
    boolean canLock = false;

    BufferedReader in = null;

    try
    {
      File lockFile = new File(task.getSource(), LOCK);

      if (!lockFile.exists())
      {
        canLock = true;
      }
      else
      {
        in = new BufferedReader(new FileReader(lockFile));

        Date date = null;

        String lockProcessId = in.readLine();

        String value = in.readLine();
        if (value != null)
        {
          date = new Date(new Long(value));
        }

        if (processId.equals(lockProcessId))
        {
          canLock = true;
        }
        else
        {
          if (date != null)
          {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(Calendar.DATE, 5);

            if (c.getTime().before(new Date()))
            {
              canLock = true;
            }
          }
        }
      }
    }
    catch (Exception e)
    {
      canLock = false;

      logger.log(LogType.ERROR, "ConfigId: " + task.getConfig().getConfigId() + " - DocumentId: " + task.getDocument().getDocumentId() + " - " + e.getMessage());
    }
    finally
    {
      if (in != null)
        try
        {
          in.close();
        }
        catch (IOException e)
        {
          logger.log(LogType.ERROR, "ConfigId: " + task.getConfig().getConfigId() + " - DocumentId: " + task.getDocument().getDocumentId() + " - " + e.getMessage());
        }
    }

    return canLock;
  }
}
