package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.scholarone.archivefiles.common.FileUtility;
import com.scholarone.archivefiles.common.S3File;
import com.scholarone.archivefiles.common.S3FileNotFoundException;
import com.scholarone.archivefiles.common.S3FileUtil;
import com.scholarone.monitoring.common.Environment;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaskLockTest
{
  RevertTask task;
  
  final String source      = "docfiles/2016/12/dev4/fse/282294";
  
  final String destination = "docfiles/dev4/fse/2015/12/282294";
  
  final String unarchiveCacheDir = ConfigPropertyValues.getProperty("unarchive.cache.dir");
  
  final File sourceDir = new File (unarchiveCacheDir + File.separator + source);

  final String sourceBucket = ConfigPropertyValues.getProperty("source.bucket.name");

  final String destinationBucket = ConfigPropertyValues.getProperty("destination.bucket.name");  

  final S3File  sourceS3Dir = new S3File(source + File.separator, sourceBucket);
  
  final S3File  destinationS3Dir = new S3File(destination + File.separator, destinationBucket);
  
  final Environment envType = Environment.getEnvironmentType("DEV");

  @Before
  public void setup() throws IOException
  {
    if (sourceDir.exists())
      FileUtils.deleteDirectory(sourceDir);
    
    int exitCode;
    
    if (S3FileUtil.isDirectory(sourceS3Dir))
    {
      exitCode = S3FileUtil.deleteS3AllVersionsRecursive(sourceS3Dir, null);
      Assert.assertTrue(exitCode == 0);
    }

    File tempDir = new File(unarchiveCacheDir);
    if (!tempDir.exists())
      tempDir.mkdirs();
    
    ArrayList<File> lists = new ArrayList<File>();
    File zipFile = new File("FSEUnarchiveTest.zip");
    UnZip unzip = new UnZip();
    unzip.extract(zipFile.getPath(), sourceDir.getPath());

    FileUtility.listDir(sourceDir, lists);
    for(File f : lists)
    {
      if (f.isDirectory())
      {
        S3FileUtil.mkdirsS3(S3FileUtil.trimKey(f.getPath()), sourceS3Dir.getBucketName());
        continue;
      }
      S3FileUtil.putFile(S3FileUtil.trimKey(f.getPath()), f, sourceS3Dir.getBucketName());
    }
    
    Integer stackId = Integer.valueOf(4);
    Config config = new Config();
    config.setConfigId(24);
    config.setShortName("fse");
    Document document = new Document();
    document.setDocumentId(282294);
    document.setFileStoreMonth(12);
    document.setFileStoreYear(2015);
    document.setArchiveMonth(12);
    document.setArchiveYear(2016);
    document.setRetryCount(0);

    Long runId = UUID.randomUUID().getLeastSignificantBits();

    task = new RevertTask(stackId, config, document, runId, ITask.NO_AUDIT, "dev", unarchiveCacheDir, sourceBucket, destinationBucket, "docfiles", envType, "dev");
  }

  @After
  public void teardown() throws IOException
  {
    File lockFile = new File(sourceDir.getPath() + File.separator + TaskLock.LOCK);
    if (lockFile.exists())
      lockFile.delete();
    S3File taskLockFile = new S3File(sourceS3Dir.getKey() + "tier3unarchive.lock", sourceS3Dir.getBucketName());
    if (S3FileUtil.exists(taskLockFile))
        S3FileUtil.deleteS3AllVersionsRecursive(taskLockFile);
  }

  @Test
  public void testALock()
  {
    ILock taskLock = new TaskLock(task, unarchiveCacheDir);
    S3File sourceS3LockFile = new S3File(sourceS3Dir.getKey(), sourceS3Dir.getBucketName());
    
    boolean result = taskLock.lock();
    Assert.assertTrue(result);
    
    result = S3FileUtil.exists(sourceS3LockFile);
    Assert.assertTrue(result);
    
    result = taskLock.lock();
    Assert.assertTrue(result);
        
    taskLock.unlock();
  }

  @Test
  public void testBLockDifferentProcess() throws S3FileNotFoundException
  {
    ILock taskLock = new TaskLock(task, unarchiveCacheDir);

    Assert.assertTrue(taskLock.lock());

    // Change process
    BufferedReader in = null;
    PrintWriter out = null;
    File lockFile = null;
    try
    {
      lockFile = S3FileUtil.getFile(task.getSourceS3Dir().getKey() + TaskLock.LOCK, task.getSourceS3Dir().getBucketName());
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

    S3FileUtil.putFile(task.getSourceS3Dir().getKey() + lockFile.getName(), lockFile, task.getSourceS3Dir().getBucketName());

    boolean result = taskLock.lock();
    Assert.assertFalse(result);
    taskLock.unlock();
  }

  @Test
  public void testCLockExpired() throws S3FileNotFoundException
  {
    ILock taskLock = new TaskLock(task, unarchiveCacheDir);

    Assert.assertTrue(taskLock.lock());

    // Change process
    BufferedReader in = null;
    PrintWriter out = null;
    File lockFile = null;

    try
    {
      lockFile = S3FileUtil.getFile(task.getSourceS3Dir().getKey() + TaskLock.LOCK, task.getSourceS3Dir().getBucketName());
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

    S3FileUtil.putFile(task.getSourceS3Dir().getKey() + lockFile.getName(), lockFile, task.getSourceS3Dir().getBucketName());
    
    boolean result = taskLock.lock();
    Assert.assertTrue(result);
    taskLock.unlock();
  }
}
