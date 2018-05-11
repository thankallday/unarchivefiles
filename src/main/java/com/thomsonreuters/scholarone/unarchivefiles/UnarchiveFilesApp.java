package com.thomsonreuters.scholarone.unarchivefiles;

import java.util.UUID;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class UnarchiveFilesApp
{
  public static Integer STACK_ID;
  
  public static Long RUN_ID;
  
  private TaskProcessor processor;
  
  void start()
  {
    processor.start();
  }
  
  public void setProcessor(TaskProcessor processor)
  {
    this.processor = processor;
  }
  
  public static void main(String[] args)
  {
    try
    {
      STACK_ID = Integer.valueOf(args[0]);
    }
    catch(Exception e)
    {
      STACK_ID = Integer.valueOf(0);
    }
    
    if ( STACK_ID < 1 || STACK_ID > 6 )
    {
      System.out.println("Usage: java -jar UnarchiveFilesApp <stack_id> (1..6)");
      return;
    }
    
    System.setProperty("stackId", STACK_ID.toString());
    
    RUN_ID = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    
    final ApplicationContext context = new ClassPathXmlApplicationContext("classpath*:applicationContext.xml");
    final UnarchiveFilesApp app = 
        context.getBean(UnarchiveFilesApp.class);
    app.start();
  }
}
