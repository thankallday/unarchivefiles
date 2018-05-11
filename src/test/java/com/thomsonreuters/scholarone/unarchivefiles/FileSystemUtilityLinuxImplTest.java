package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scholarone.activitytracker.TrackingInfo;

public class FileSystemUtilityLinuxImplTest
{
  String source = "/shared/gus_archive/docfiles/2016/12/dev4/fse/282294";

  String destination = "/shared/gus/docfiles/dev4/fse/2016/12";

  @Before
  public void setup() throws IOException
  {
    File zipFile = new File("FSEUnarchiveTest.zip");
    UnZip unzip = new UnZip();
    unzip.extract(zipFile.getPath(), source);
  }

  @After
  public void teardown() throws IOException
  {
    File sourceFile = new File(source);
    File destFile = new File(destination + "/282294");
    
    FileUtils.deleteDirectory(sourceFile);    
    FileUtils.deleteDirectory(destFile);
  }

  @Test
  public void testCopy() throws IOException
  {
    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(4);
    TrackingInfo stat = new TrackingInfo();
    int exitCode = fs.copy(source, destination, stat);
    Assert.assertTrue(exitCode == 0);
    
    File destFile1 = new File(destination + "/282294/docfiles/", "_system_appendPDF_proof_hi.pdf");
    File destFile2 = new File(destination + "/282294/docfiles/", "_system_appendPDF_proof_output_test_only.pdf");
    File destFile3 = new File(destination + "/282294/docfiles/", "1-s2.0-S0144861713011223-main[paper1].pdf");
    File destFile4 = new File(destination + "/282294/docfiles/notes/5455", "FINAL-tropical- after second modification 15-7-2012[1].doc");
    File destFile5 = new File(destination + "/282294/docfiles/notes/345206", "[Linear Algebra re 10,4. Welles. 7,1. 7,1. 2015.] (Figures).pdf");
    File destFile6 = new File(destination + "/282294/docfiles/notes/5455", "- nl-test.pdf");
    File destFile7 = new File(destination + "/282294/docfiles/original-files", "502676_File000003_4095174.0]");
    File destFile8 = new File(destination + "/282294/docfiles/notes/5455", "#2_Second response to reviewers(JSLee).doc");
    File destFile9 = new File(destination + "/282294/docfiles/notes/5455", "jz201001661U-Reviewer#4.pdf");
    File destFile10 = new File(destination + "/282294/docfiles/notes/5455", "#7bis.tex");
    File destFile11 = new File(destination + "/282294/docfiles/notes/5455", "\" jm-2012-01013n.R1.pdf");
    File destFile12 = new File(destination + "/282294/docfiles/notes/5455", "$7M to spend 8-18[1]-Bradley.docx");
    File destFile13 = new File(destination + "/282294/docfiles/tex/home/rbb/sources/LaTeX/bibtex/bst/", "nf.bst");
    
    Assert.assertTrue(destFile1.exists());
    Assert.assertTrue(destFile2.exists());
    Assert.assertTrue(destFile3.exists());
    Assert.assertTrue(destFile4.exists());
    Assert.assertTrue(destFile5.exists());
    Assert.assertTrue(destFile6.exists());
    Assert.assertTrue(destFile7.exists());
    Assert.assertTrue(destFile8.exists());
    Assert.assertTrue(destFile9.exists());
    Assert.assertTrue(destFile10.exists());
    Assert.assertTrue(destFile11.exists());
    Assert.assertTrue(destFile12.exists());
    Assert.assertTrue(destFile13.exists());
    
  }

  @Test
  public void testCopyMissingDirectory() throws IOException
  {
    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(4);
    int exitCode = fs.copy("/shared/gus_archive/docfiles/2016/12/dev4/missing_directory", destination);

    Assert.assertTrue(exitCode == 23);
  }
  
  @Test
  public void testDelete() throws IOException
  {
    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(4);
    int exitCode = fs.delete(source, true);

    Assert.assertTrue(exitCode == 0);
  }
  
  @Test
  public void testDeleteMissingDirectory() throws IOException
  {
    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(4);
    int exitCode = fs.delete("/shared/gus_archive/docfiles/2016/12/dev4/missing_directory", true);

    Assert.assertTrue(exitCode == 0);
  }
  
  @Test
  public void testCompleteMove() throws IOException
  {
    IFileSystemUtility fs = new FileSystemUtilityLinuxImpl(4);

    int exitCode = fs.copy(source, destination);
    Assert.assertTrue(exitCode == 0);

    exitCode = fs.delete(source, true);
    Assert.assertTrue(exitCode == 0);
  }
}
