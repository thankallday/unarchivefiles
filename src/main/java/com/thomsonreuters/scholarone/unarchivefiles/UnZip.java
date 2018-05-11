package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnZip
{
  List<String> fileList;

  /**
   * Unzip it
   * 
   * @param zipFile
   *          input zip file
   * @param output
   *          zip file output folder
   */
  public boolean extract(String zipFile, String outputFolder)
  {
    boolean wasExtracted = false;
    
    byte[] buffer = new byte[1024];

    try
    {

      // create output directory is not exists
      File folder = new File(outputFolder);
      if (!folder.exists())
      {
        folder.mkdir();
      }

      // get the zip file content
      ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
      // get the zipped file list entry
      ZipEntry ze = zis.getNextEntry();

      while (ze != null)
      {
        String name = ze.getName();
        File newFile = new File(outputFolder + File.separator + name);
        
        if ( ze.isDirectory() )
        {
          newFile.mkdirs();
        }
        else
        {
          // create all non exists folders
          // else you will hit FileNotFoundException for compressed folder
          new File(newFile.getParent()).mkdirs();
  
          FileOutputStream fos = new FileOutputStream(newFile);
  
          int len;
          while ((len = zis.read(buffer)) > 0)
          {
            fos.write(buffer, 0, len);
          }
  
          fos.close();
        }
        
        wasExtracted = true;
        ze = zis.getNextEntry();
      }

      zis.closeEntry();
      zis.close();
    }
    catch (IOException ex)
    {
      wasExtracted = false;
      ex.printStackTrace();
    }
    
    return wasExtracted;
  }
}
