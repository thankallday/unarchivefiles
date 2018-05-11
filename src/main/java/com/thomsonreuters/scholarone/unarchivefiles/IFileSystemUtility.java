package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.IOException;

import com.scholarone.activitytracker.TrackingInfo;

public interface IFileSystemUtility
{
  public int copy(String source, String destination) throws IOException;
  
  public int copy(String source, String destination, TrackingInfo stat) throws IOException;
  
  public int delete(String source, boolean recurse) throws IOException;
  
}
