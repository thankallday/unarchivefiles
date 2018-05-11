package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.thomsonreuters.scholarone.unarchivefiles.ConfigPropertyValues;

public class ConfigPropertyValuesTest
{
  String sourceProp = "config.properties";
  String testProp = "config.properties.values.test";
  
  @Before
  public void setup() throws IOException
  {
    File srcFile = new File(sourceProp);
    File destFile = new File(testProp);
    
    FileUtils.copyFile(srcFile, destFile);
    
    FileInputStream in = new FileInputStream(testProp);
    Properties props = new Properties();
   
    props.load(in);   
    in.close();
  
    FileOutputStream out = new FileOutputStream(testProp);
    props.setProperty("test.property", "test_value");
    props.store(out, null);
    out.close();
  }
  
  @After
  public void teardown()
  {
    File destFile = new File(testProp);
    FileUtils.deleteQuietly(destFile);
  }
  
  //@Test
  public void testGetProperty()
  {
    try
    {
      FileInputStream in = new FileInputStream(testProp);
      Properties props = new Properties();
      props.load(in);
      
      String test = props.getProperty("test.property");
      Assert.assertEquals(test, ConfigPropertyValues.getProperty("test.property", testProp));
      
      in.close();
      
      in = new FileInputStream(testProp);
      props = new Properties();
      props.load(in);   
      in.close();
    
      FileOutputStream out = new FileOutputStream(testProp);
      props.setProperty("test.property", test + "2");
      props.store(out, null);
      out.close();
      
      Assert.assertEquals(test, ConfigPropertyValues.getProperty("test.property", testProp));
      
      Thread.sleep(120000);
      
      Assert.assertNotEquals(test, ConfigPropertyValues.getProperty("test.property", testProp));
    }
    catch (IOException ioe)
    {
      Assert.fail(ioe.getMessage());
    }
    catch (InterruptedException e)
    {
      Assert.fail(e.getMessage());
    }
  }
}
