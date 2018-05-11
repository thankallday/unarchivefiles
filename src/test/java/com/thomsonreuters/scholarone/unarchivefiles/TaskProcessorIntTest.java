package com.thomsonreuters.scholarone.unarchivefiles;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.scholarone.archivefiles.common.FileUtility;
import com.scholarone.archivefiles.common.S3File;
import com.scholarone.archivefiles.common.S3FileUtil;
import com.scholarone.monitoring.common.Environment;

@RunWith(SpringJUnit4ClassRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@ContextConfiguration("classpath:applicationContextTest.xml")
public class TaskProcessorIntTest
{
  final String source      = "docfiles/2016/12/dev4/fse/282294";

  final String destination = "docfiles/dev4/fse/2015/12/282294";

  final String unarchiveCacheDir = ConfigPropertyValues.getProperty("unarchive.cache.dir");
  
  final File sourceDir = new File (unarchiveCacheDir + File.separator + source);

  final String sourceBucket = ConfigPropertyValues.getProperty("source.bucket.name");
  
  final String destinationBucket  = ConfigPropertyValues.getProperty("destination.bucket.name");

  final S3File sourceS3Dir = new S3File(source + File.separator, sourceBucket);

  final S3File destinationS3Dir = new S3File(destination + File.separator, destinationBucket);
  
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
    
    if (S3FileUtil.isDirectory(destinationS3Dir))
    {
      exitCode = S3FileUtil.deleteS3AllVersionsRecursive(destinationS3Dir, null);
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
    
    UnarchiveFilesDAOTestImpl db = new UnarchiveFilesDAOTestImpl();
    db.openConnection(4);
    
    db.deleteDocument();
    db.addDocument();
    
    db.closeConnection();
  }
  
  @After
  public void teardown() throws IOException
  {
    UnarchiveFilesDAOTestImpl db = new UnarchiveFilesDAOTestImpl();
    db.openConnection(4);    
    db.deleteDocument();
    db.closeConnection();

    File lockFile = new File(sourceDir.getPath() + File.separator + TaskLock.LOCK);
    if (lockFile.exists())
      lockFile.delete();
    
    if (S3FileUtil.isDirectory(sourceS3Dir))
      S3FileUtil.deleteS3AllVersionsRecursive(sourceS3Dir, null);
    
    if (S3FileUtil.isDirectory(destinationS3Dir))
      S3FileUtil.deleteS3AllVersionsRecursive(destinationS3Dir, null);
  }
  
  //CAUTION: Should comment S3FileUtil.shutdownS3Daemons(); in TaskProcessor.java. If not, http reaper deamon is killed. So more Assert is impossible.
  @Test
  public void testProcess() throws IOException
  {
    List<ITask> tasks = new ArrayList<ITask>();

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
    ITask task = new RevertTask(stackId, config, document, runId, ITask.NO_AUDIT, "dev", unarchiveCacheDir, sourceBucket, destinationBucket, "docfiles", envType, "dev");
    tasks.add(task);
    
    TaskProcessor p = new TaskProcessor(stackId, runId, new TaskFactoryImpl(stackId, runId));
    p.setTasks(tasks);
    p.start();

    try
    {
      while (p.isAlive())
      {
        Thread.sleep(300);
      }
    }
    catch (InterruptedException ie)
    {
      fail(ie.getMessage());
    }
    
    Assert.assertTrue(1 == p.getCompletedCount());
    
    boolean result;
    
    S3File test1 = new S3File(destinationS3Dir.getKey() + "docfiles/original-files/282294_File000000_4675773.doc", destinationS3Dir.getBucketName());
    result = S3FileUtil.exists(test1);
    Assert.assertTrue(result);
    
    S3File test2 = new S3File(destinationS3Dir.getKey() + "docfiles/original-files/282294_File000004_4675785.jpg", destinationS3Dir.getBucketName());
    result = S3FileUtil.exists(test2);
    Assert.assertTrue(result);

    S3File test3 = new S3File(destinationS3Dir.getKey() + "docfiles/original-files/282294_File000001_4675776.xlsx", destinationS3Dir.getBucketName());
    result = S3FileUtil.exists(test3);
    Assert.assertTrue(result);
    
    S3File test4 = new S3File(destinationS3Dir.getKey() + "exclude_from.txt", destinationS3Dir.getBucketName());
    result = S3FileUtil.exists(test4);
    Assert.assertFalse(result);
    
    S3File test5 = new S3File(destinationS3Dir.getKey() + TaskLock.LOCK, destinationS3Dir.getBucketName());
    result = S3FileUtil.exists(test5);
    Assert.assertFalse(result);    
  }
  
}
