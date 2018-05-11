package com.thomsonreuters.scholarone.unarchivefiles.audit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.scholarone.archivefiles.common.S3File;
import com.scholarone.archivefiles.common.S3FileUtil;
import com.thomsonreuters.scholarone.unarchivefiles.TaskLock;


public class S3FileAuditUtility
{ 
  /*
   * Returns a map, keyed by filename and a list of paths containing a file with that name. This is done to handle
   * duplicate file names
   */
  public static Map<String, List<String>> getFilesAndPathsAsMap(S3File basePath)
  {
    Map<String, List<String>> fileMap = new HashMap<String, List<String>>();

    if (S3FileUtil.isDirectory(basePath))
    {
      addFilesToMap(basePath, fileMap);
    }
    else
    {
      return fileMap;
    }

    return fileMap;
  }

  /*
   * Recursive method to traverse directories and build the map
   */
  private static void addFilesToMap(S3File path, Map<String, List<String>> fileMap)
  {
    if (!S3FileUtil.isDirectory(path)) return;

    S3File[] fileList = S3FileUtil.listFiles(path);
    if (fileList != null)
    {
      for (int i = 0; i < fileList.length; i++)
      {
        S3File f = fileList[i];
        
        if (f.getKey().endsWith("exclude_from.txt") || f.getKey().endsWith("tier3move.lock") || f.getKey().endsWith(TaskLock.LOCK))
          continue;
        
        if (!S3FileUtil.isDirectory(f))
        {
          if (!fileMap.containsKey(f.getKey()))
          {
            List<String> pathList = new ArrayList<String>();
            fileMap.put(S3FileUtil.getFileName(f), pathList);
          }
          fileMap.get(S3FileUtil.getFileName(f)).add(S3FileUtil.getParentPath(f));
        }
        else
        {
          addFilesToMap(f, fileMap);
        }
      }
    }
  }
}
