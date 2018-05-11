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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContextTest.xml")
public class TaskProcessorIntTest
{  
  String source = "/shared/gus_archive/docfiles/2016/12/dev4/fse/282294";

  String destination = "/shared/gus/docfiles/dev4/fse/2016/12";

  @Before
  public void setup() throws IOException
  {
    File zipFile = new File("FSEUnarchiveTest.zip");
    UnZip unzip = new UnZip();
    unzip.extract(zipFile.getPath(), source);
    
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
    
    File sourceFile = new File(source);
    File destFile = new File(destination + "/282294");
    
    FileUtils.deleteDirectory(sourceFile);    
    FileUtils.deleteDirectory(destFile);
  }
  
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
    document.setFileStoreYear(2016);
    document.setArchiveMonth(12);
    document.setArchiveYear(2016);
    document.setRetryCount(0);
    
    Long runId = UUID.randomUUID().getLeastSignificantBits();
    ITask task = new RevertTask(stackId, config, document, runId, ITask.NO_AUDIT);
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
    
    File test1 = new File(destination + "/282294/docfiles/original-files/", "282294_File000000_4675773.doc");
    Assert.assertTrue(test1.exists());
    
    File test2 = new File(destination + "/282294/docfiles/original-files/", "282294_File000004_4675785.jpg");
    Assert.assertTrue(test2.exists());
    
    File test3 = new File(destination + "/282294/docfiles/original-files/", "282294_File000001_4675776.xlsx");
    Assert.assertTrue(test3.exists());
    
    File test4 = new File(destination + "/282294/", "exclude_from.txt");
    Assert.assertFalse(test4.exists());
    
    File test5 = new File(destination + "/282294/", "tier3move.lock");
    Assert.assertFalse(test5.exists());
  }
  
}
