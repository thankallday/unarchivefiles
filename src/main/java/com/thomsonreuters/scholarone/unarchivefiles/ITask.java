package com.thomsonreuters.scholarone.unarchivefiles;

public interface ITask extends Runnable
{
  public static final int NO_AUDIT = 0;
  
  public static final int AUDIT = 1;
  
  public int getExitCode();
  
  public Double getTransferRate();
  
  public Long getTransferTime();

  public Long getTransferSize();

  public void run();
}
