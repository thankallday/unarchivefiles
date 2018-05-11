package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.thomsonreuters.scholarone.unarchivefiles.Config;
import com.thomsonreuters.scholarone.unarchivefiles.Document;
import com.thomsonreuters.scholarone.unarchivefiles.ILock;
import com.thomsonreuters.scholarone.unarchivefiles.ITask;
import com.thomsonreuters.scholarone.unarchivefiles.RevertTask;
import com.thomsonreuters.scholarone.unarchivefiles.TaskLock;
import com.thomsonreuters.scholarone.unarchivefiles.UnZip;

public class TaskLockTest
{
  RevertTask task;

  String source = "/shared/gus_archive/docfiles/2016/12/dev4/fse/282294";

  @Before
  public void setup() throws IOException
  {
    File zipFile = new File("FSEUnarchiveTest.zip");
    UnZip unzip = new UnZip();
    unzip.extract(zipFile.getPath(), source);

    Integer stackId = Integer.valueOf(4);
    Config config = new Config();
    config.setConfigId(24);
    config.setShortName("fse");
    Document document = new Document();
    document.setDocumentId(282294);
    document.setFileStoreMonth(12);
    document.setFileStoreYear(2016);
    document.setArchiveMonth(12);
    document.setArchiveYear(2016);
    document.setRetryCount(0);

    Long runId = UUID.randomUUID().getLeastSignificantBits();
    
    task = new RevertTask(stackId, config, document, runId, ITask.NO_AUDIT);
  }

  @After
  public void teardown() throws IOException
  {
    File sourceFile = new File(source);
    FileUtils.deleteDirectory(sourceFile);
  }

  @Test
  public void testLock()
  {
    ILock taskLock = new TaskLock(task);

    Assert.assertTrue(taskLock.lock());
    Assert.assertTrue(taskLock.lock());
    taskLock.unlock();
  }

  @Test
  public void testLockDifferentProcess()
  {
    ILock taskLock = new TaskLock(task);

    Assert.assertTrue(taskLock.lock());

    // Change process
    BufferedReader in = null;
    PrintWriter out = null;

    try
    {
      File lockFile = new File(task.getSource(), TaskLock.LOCK);
      in = new BufferedReader(new FileReader(lockFile));
      String process = in.readLine();
      String time = in.readLine();
      in.close();

      out = new PrintWriter(lockFile);
      out.println(process + "xyz");
      out.println(time);
      out.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (in != null) in.close();
        if (out != null) out.close();
      }
      catch (IOException ex)
      {
        ex.printStackTrace();
      }
    }

    Assert.assertFalse(taskLock.lock());
    taskLock.unlock();
  }

  @Test
  public void testLockExpired()
  {
    ILock taskLock = new TaskLock(task);

    Assert.assertTrue(taskLock.lock());

    // Change process
    BufferedReader in = null;
    PrintWriter out = null;

    try
    {
      File lockFile = new File(task.getSource(), TaskLock.LOCK);
      in = new BufferedReader(new FileReader(lockFile));
      String process = in.readLine();
      in.close();

      out = new PrintWriter(lockFile);
      out.println(process + "xyz");
      out.println("1");
      out.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (in != null) in.close();
        if (out != null) out.close();
      }
      catch (IOException ex)
      {
        ex.printStackTrace();
      }
    }

    Assert.assertTrue(taskLock.lock());
    taskLock.unlock();
  }
}
