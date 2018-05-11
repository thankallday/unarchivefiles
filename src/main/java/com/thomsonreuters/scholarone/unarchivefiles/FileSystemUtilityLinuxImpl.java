package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.scholarone.activitytracker.ILog;
import com.scholarone.activitytracker.TrackingInfo;
import com.scholarone.activitytracker.ref.LogTrackerImpl;
import com.scholarone.activitytracker.ref.LogType;

public class FileSystemUtilityLinuxImpl implements IFileSystemUtility
{
  private static ILog logger = null;

  private Integer stackId;
  
  public FileSystemUtilityLinuxImpl(Integer stackId)
  {
    if (logger == null)
    {
      logger = new LogTrackerImpl(this.getClass().getName());
    }
    
    this.stackId = stackId;
  }

  public int copy(String source, String destination) throws IOException
  {
    return copy(source, destination, null);
  }

  public int copy(String source, String destination, TrackingInfo stat)
      throws IOException
  {
    File destFile = new File(destination);
    if (!destFile.exists())
    {
      destFile.mkdirs();
    }

    List<String> commands = new ArrayList<String>();
    commands.add("rsync");
    commands.add("-arvP");    
    commands.add("-stats");

    commands.add(source);
    commands.add(destination);

    ProcessBuilder pb = new ProcessBuilder().command(commands);
    pb.redirectOutput();
    Process iostat = pb.start();

    if (stat != null) ParseRSYNC.parse(iostat.getInputStream(), stat, stackId);

    int exitCode = 0;

    try
    {
      logger.log(LogType.INFO, "command-s " + commands);
      exitCode = iostat.waitFor();
      logger.log(LogType.INFO, "command-e " + commands);
      if ( exitCode != 0 )
      {
        logger.log(LogType.ERROR, "Failed to copy. Exit code: " + exitCode + ", Command=" + commands );
      }
    }
    catch (InterruptedException e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }

    return exitCode;
  }

  public int delete(String source, boolean recurse) throws IOException
  {
    String[] command = { "rm", (recurse ? "-rf" : "-f"), source };

    Process iostat = new ProcessBuilder().command(command).inheritIO().start();

    int exitCode = 0;

    try
    {
      exitCode = iostat.waitFor();
      if ( exitCode != 0 )
      {
        logger.log(LogType.ERROR, "Failed to delete. Exit code: " + exitCode + ", Command=" + command );
      }
    }
    catch (InterruptedException e)
    {
      logger.log(LogType.ERROR, e.getMessage());
    }

    return exitCode;
  }

}
