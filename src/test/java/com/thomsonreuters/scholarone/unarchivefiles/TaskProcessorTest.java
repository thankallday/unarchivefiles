package com.thomsonreuters.scholarone.unarchivefiles;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.thomsonreuters.scholarone.unarchivefiles.ITask;
import com.thomsonreuters.scholarone.unarchivefiles.TaskFactoryImpl;
import com.thomsonreuters.scholarone.unarchivefiles.TaskProcessor;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContextTest.xml")
public class TaskProcessorTest
{  
  @Test
  public void testProcess()
  {
    long count = 1000;
    
    List<ITask> tasks = new ArrayList<ITask>();

    for (long i = 0; i < count; i++)
    {
      tasks.add(new RevertTaskTest(50));
    }
    
    Long runId = UUID.randomUUID().getLeastSignificantBits();
    
    TaskProcessor p = new TaskProcessor(4, runId, new TaskFactoryImpl(4, runId));
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
    
    Assert.assertTrue(count == p.getCompletedCount());
  }
  
  @Test
  public void testProcessStop()
  {
    long count = 10000;
    
    List<ITask> tasks = new ArrayList<ITask>();

    for (long i = 0; i < count; i++)
    {
      tasks.add(new RevertTaskTest(50));
    }
    
    Long runId = UUID.randomUUID().getLeastSignificantBits();
    
    TaskProcessor p = new TaskProcessor(4, runId, new TaskFactoryImpl(4, runId));
    p.setTasks(tasks);
    p.start();

    try
    {
      while (p.isAlive())
      {
        Thread.sleep(300);
        if ( !p.isStop() )
          p.setStop(true);
      }
    }
    catch (InterruptedException ie)
    {
      fail(ie.getMessage());
    }
    
    System.out.println("Completed: " + p.getCompletedCount());
    Assert.assertTrue(count > p.getCompletedCount());
  }
}
