package com.scholarone.archivefiles.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.client.builder.ExecutorFactory;

public class S3ExecutorFactory implements ExecutorFactory
{
  public ExecutorService newExecutor()
  {
    // TODO Auto-generated method stub
    return Executors.newFixedThreadPool(5);
  }

}
