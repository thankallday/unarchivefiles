package com.scholarone.archivefiles.common;

import java.io.File;
import java.util.ArrayList;


/**
 * This class contains various utility functions that are useful when dealing with files.
 * 
 * @author joby.knuth
 */
public class FileUtility
{
  public static void listDir(File file, ArrayList<File> lists)
  {
    if (file.isDirectory())
    {
      if (file.list().length == 0)
      {
        lists.add(file);
      }
      else
      {
        File[] files = file.listFiles();
        for(File f : files)
        {
          if (f.isDirectory())
            listDir(f, lists);
          else
            lists.add(f);
        } 
      }
    }
    else
      lists.add(file);
  }
}
