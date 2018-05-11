package com.thomsonreuters.scholarone.unarchivefiles;

import com.thomsonreuters.scholarone.unarchivefiles.ITask;


public class RevertTaskTest implements ITask
{
  private Integer sleepTime;
  
  private int exitCode = -1;
  
  public RevertTaskTest(Integer sleepTime)
  {
    this.sleepTime = sleepTime;
  }
  
  public int getExitCode()
  {
    return exitCode;
  }
  
  @Override
  public void run()
  {
    try
    {
      Thread.sleep(sleepTime);
      exitCode = 0;
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  @Override
  public Long getTransferTime()
  {
    return null;
  }

  @Override
  public Long getTransferSize()
  {
    return null;
  }
  
  public Double getTransferRate()
  {
    return null;
  }

}
