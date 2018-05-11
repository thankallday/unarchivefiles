package com.thomsonreuters.scholarone.unarchivefiles.audit;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileAuditUtility
{ 
  /*
   * Returns a map, keyed by filename and a list of paths containing a file with that name. This is done to handle
   * duplicate file names
   */
  public static Map<String, List<String>> getFilesAndPathsAsMap(String basePath)
  {
    Map<String, List<String>> fileMap = new HashMap<String, List<String>>();
    File path = new File(basePath);

    if (path.isDirectory())
    {
      addFilesToMap(path, fileMap);
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
  private static void addFilesToMap(File path, Map<String, List<String>> fileMap)
  {
    if (!path.isDirectory()) return;
    File[] fileList = path.listFiles();
    for (int i = 0; i < fileList.length; i++)
    {
      File f = fileList[i];
      if (!f.isDirectory())
      {
        if (!fileMap.containsKey(f.getName()))
        {
          List<String> pathList = new ArrayList<String>();
          fileMap.put(f.getName(), pathList);
        }
        fileMap.get(f.getName()).add(f.getParent());
      }
      else
        addFilesToMap(f, fileMap);
    }
  }
}
