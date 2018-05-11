package com.scholarone.archivefiles.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.scholarone.monitoring.common.Environment;
import com.scholarone.monitoring.common.IMetricSubTypeConstants;
import com.scholarone.monitoring.common.MetricProduct;
import com.scholarone.monitoring.common.PublishMetrics;
import com.scholarone.monitoring.common.ServiceComponent;
import com.thomsonreuters.scholarone.unarchivefiles.ConfigPropertyValues;
import com.thomsonreuters.scholarone.unarchivefiles.StatInfo;
import com.thomsonreuters.scholarone.unarchivefiles.TaskLock;



public class S3FileUtil
{
  protected static Logger log = Logger.getLogger(S3FileUtil.class.getName());

  private static ClientConfiguration clientConfig = new ClientConfiguration();

  private static String awsRegion = ConfigPropertyValues.getProperty("aws.region");

  private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                                      .withRegion(awsRegion)
                                      .withClientConfiguration(clientConfig).build();
  
  private static TransferManager transferManager = AWSClientFactory.getInstance().getTransferManager();

  private static String CACHE_PATH = ConfigPropertyValues.getProperty("unarchive.cache.dir");

  private static String directoryPattern = ".*/";

  private static Pattern pattern = Pattern.compile(directoryPattern);
  
  private static String envName = ConfigPropertyValues.getProperty("environment");

  private static Environment envType = Environment.getEnvironmentType("DEV");
  
  public static String EXCLUDED_FILE = "exclude_from.txt";

  public static String trimKey(String key)
  {
    if (key != null)
      key = key.replace(CACHE_PATH + File.separator, "");   
    return key;
  }
  
  public static File getFile(String key, String bucketName) throws S3FileNotFoundException
  {
    File targetFile = new File(CACHE_PATH + File.separator + key);
    
    if (!(targetFile.getParentFile()).exists())
      targetFile.getParentFile().mkdirs();
    
    log.debug("CACHE-FILE= " + targetFile.getPath() + File.separator + targetFile.getName());
  
    try
    {
      log.debug("Getting Object:: bucket= " + bucketName + ", key= " + key);
  
      S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
      if (object != null)
      {
        InputStream inStream = object.getObjectContent();
  
        OutputStream outStream = new FileOutputStream(targetFile);
  
        byte[] buffer = new byte[8 * 1024];
        int bytesRead;
        while ((bytesRead = inStream.read(buffer)) != -1)
        {
          outStream.write(buffer, 0, bytesRead);
        }
        outStream.close();
        inStream.close();
  
        return targetFile;
      }
    }
    catch (SdkClientException sdkClientExcpetion)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5001 | GET-FILE-ERROR | S3FileUtil | (" + bucketName + ":" + key + ") | "
          + sdkClientExcpetion.getMessage() + " ]");
    }
    catch (AmazonClientException amazonClientException)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5001 | GET-FILE-ERROR | S3FileUtil | (" + bucketName + ":" + key + ") | "
          + amazonClientException.getMessage() + " ]");
    }
    catch(IOException ioe)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5001 | GET-FILE-ERROR | S3FileUtil | (" + bucketName + ":" + key + ") | "
          + ioe.getMessage() + " ]");
    }
  
    throw new S3FileNotFoundException("(" + bucketName + ":" + key + ")");
  }
  
  public static void putFile(S3File s3File, File file) throws S3FileNotFoundException
  {
    putFile(s3File.getKey(), file, s3File.getBucketName());
  }
  
  public static void putFile(String key, File file, String bucketName) throws S3FileNotFoundException
  {
    long start = System.currentTimeMillis();

    key = trimKey(key);

    try
    {      
      log.debug("Uploading Object:: bucket= " + bucketName + ", key= " + key);

      SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a z");
      sdf.setTimeZone(TimeZone.getTimeZone("EST"));

      ObjectMetadata metadata = new ObjectMetadata();
      metadata.addUserMetadata("original-date", sdf.format(file.lastModified()));

      PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file).withMetadata(metadata);

      // TransferManager processes all transfers asynchronously,
      // so this call will return immediately.
      Upload upload = transferManager.upload(putRequest);

      // Or you can block and wait for the upload to finish
      upload.waitForCompletion();

      long end = System.currentTimeMillis();
      log.info("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5002 | PUT-FILE-SUCCESS | S3FileUtil | " + "(" + bucketName + ":" + key + ")"
          + " | " + (end - start) + " ms ]");

    }
    catch(AmazonServiceException ase)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5002 | PUT-FILE-ERROR | S3FileUtil | "
          + "(" + bucketName + ":" + key + ")" + " | " + ase.getMessage() + " ]");
      throw new S3FileNotFoundException("(" + bucketName + ":" + key + ")");
    }
    catch (AmazonClientException amazonClientException)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5002 | PUT-FILE-ERROR | S3FileUtil | "
          + "(" + bucketName + ":" + key + ")" + " | " + amazonClientException.getMessage() + " ]");
      throw new S3FileNotFoundException("(" + bucketName + ":" + key + ")");
    }
    catch (InterruptedException e)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5002 | PUT-FILE-ERROR | S3FileUtil | "
          + "(" + bucketName + ":" + key + ")" + " | " + e.getMessage() + " ]");
      throw new S3FileNotFoundException("(" + bucketName + ":" + key + ")");
    }
  }
  
  public static int removeFile(S3File file)
  {
    return removeFile(file.getKey(), file.getBucketName());
  }
  
  /*
   * Delete the latest version of file. To delete all versions of file, use deleteS3DirRecursive.  
   */
  public static int removeFile(String key, String bucketName)
  {
    log.debug("Deleting Object:: bucket= " + bucketName + ", key= " + key);
  
    try
    {
      s3Client.deleteObject(new DeleteObjectRequest(bucketName, key));
      log.info("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5003 | DELETE-FILE-SUCCESS | S3FileUtil | " + "(" + bucketName + ":" + key + ")" + " ]");
    }
    catch (AmazonServiceException amazonServiceException)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5003 | DELETE-FILE-ERROR | S3FileUtil | " + "(" + bucketName + ":" + key + ")" + " | "
          + amazonServiceException.getMessage() + " ]");
      return -1;
    }
    catch(SdkClientException sce)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5003 | DELETE-FILE-ERROR | S3FileUtil | " + "(" + bucketName + ":" + key + ")" + " | "
          + sce.getMessage() + " ]");
      return -2;
    }
    return 0;
  }

  public static int copyS3Dir(S3File sourceS3Dir, S3File destinationS3Dir, ArrayList<String> exclusionList, StatInfo stat)
  {
    return copyS3Dir(sourceS3Dir, destinationS3Dir, exclusionList, null, stat);
  }

  public static int copyS3Dir(S3File sourceS3Dir, S3File destinationS3Dir, ArrayList<String> exclusionList, StorageClass storageClass, StatInfo stat)
  {
    String destFullPath = "";
    Long size = 0L;
    Long transferTime = 0L;
    String fileKeys[] = sourceS3Dir.listFileKeys();
    if (fileKeys != null)
    {
      stat.setNumOfFilesTotal(fileKeys.length);
      
      if (storageClass == null)
        storageClass = StorageClass.Standard;
      
      if (fileKeys.length == 0)
      {
        PublishMetrics.incrementTotalCount  (MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
        PublishMetrics.incrementSuccessCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
      }
      for (int i = 0; i < fileKeys.length; i++)
      {
        String srcFullPath = fileKeys[i];
        
        if (exclusionList != null && isExcluded(getFileName(srcFullPath),  exclusionList))
        {
          stat.decreaseNumOfFilesTotal();
          continue;
        }

        if (srcFullPath.endsWith("exclude_from.txt") || srcFullPath.endsWith("tier3move.lock") || srcFullPath.endsWith(TaskLock.LOCK))
        {
          stat.decreaseNumOfFilesTotal();
          continue;
        }
        
        String subPath = srcFullPath.replaceAll(sourceS3Dir.getKey(), "");
        destFullPath = destinationS3Dir.getKey() + subPath;

        try 
        {
          PublishMetrics.incrementTotalCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
          
          CopyObjectRequest copyObjRequest = new CopyObjectRequest(sourceS3Dir.getBucketName(), srcFullPath, destinationS3Dir.getBucketName(), destFullPath);
          copyObjRequest.setStorageClass(storageClass);
          
          long start = System.currentTimeMillis();
          
          CopyObjectResult cor = s3Client.copyObject(copyObjRequest);
          long end = System.currentTimeMillis();
          long duration = end - start; // ms
          transferTime = transferTime + duration;
          PublishMetrics.logExecutionTime(MetricProduct.S1M, (int)(end - start), ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
          if (cor == null | cor.getETag() == null)
          {
            PublishMetrics.incrementFailureCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
          }
          else
          {          
            Long bytes = getFileSize(destFullPath, destinationS3Dir.getBucketName());
            if (bytes < 0L)
            {
              PublishMetrics.incrementFailureCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
            }
            else
            {
              size = size + bytes;             
              stat.increaseNumOfFilesSuccess();
              PublishMetrics.incrementSuccessCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
              log.info("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5004 | COPY-DIR-SUCCESS | S3FileUtil | " + srcFullPath + " | " + destFullPath + " ]");
            }
          }
        }
        catch (AmazonServiceException ase)
        {
            // Caught an AmazonServiceException, which means your request made it to Amazon S3, but was rejected with an error response for some reason.
            stat.increaseNumOfFilesFailure();
            PublishMetrics.incrementErrorCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
            log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5004 | COPY-DIR-ERROR | S3FileUtil | " + srcFullPath + " | " + destFullPath + " | "  
                + ase.getMessage() + " ]");
        }
        catch(SdkClientException sce)
        {
          stat.increaseNumOfFilesFailure();
          PublishMetrics.incrementErrorCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
          log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5004 | COPY-DIR-ERROR | S3FileUtil | " + srcFullPath + " | " + destFullPath + " | "  
              + sce.getMessage() + " ]");
        }
        catch (AmazonClientException ace)
        {
            //Caught an AmazonClientException, which means the client encountered an internal error while trying to communicate with S3, such as not being able to access the network.
            stat.increaseNumOfFilesFailure();
            PublishMetrics.incrementErrorCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
            log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5004 | COPY-DIR-ERROR | S3FileUtil | " + srcFullPath + " | " + destFullPath + " | "  
                + ace.getMessage() + " ]");
        }
      }
    }
    else
    {
      stat.increaseNumOfFilesFailure();
      PublishMetrics.incrementFailureCount(MetricProduct.S1M, ServiceComponent.UNARCHIVE_S1SVC.getComponent(), IMetricSubTypeConstants.ARCHIVE_FILES, envType, envName);
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5005 | COPY-DIR-ERROR | S3FileUtil | " + sourceS3Dir.getKey() + " | "
          + "directory not found"  + " ]");
    }

    stat.setTransferSize(size);
    stat.setTransferTime(transferTime);
    double transferRate = (double) (size / ((double) transferTime / 1000));
    stat.setTransferRate(transferRate);

    if (stat.getNumOfFilesTotal().intValue() != stat.getNumOfFilesSuccess().intValue())
    {
      log.error("COPY-DIR-ERROR documentId=" + stat.getObjectId() + ", total=" + stat.getNumOfFilesTotal() + ", failure=" + stat.getNumOfFilesSuccess() + " | " + sourceS3Dir.getKey());
      return -1;
    }
    else
      return 0;
  }

  //delete directories except files in exclusionList
  public static int deleteS3AllVersionsRecursive(S3File directory)
  {
    return deleteS3AllVersionsRecursive(directory, null);
  }

  public static int deleteS3AllVersionsRecursive(S3File directory, ArrayList<String> exclusionList)
  {
    S3File[] files = directory.listVersions();
    if (files != null)
    {
      for (int i = 0; i < files.length; i++)
      {
        S3File file = files[i];

        if (isExcluded(S3FileUtil.getFileName(file), exclusionList))
          continue;

        try
        {
          s3Client.deleteVersion( file.getBucketName(), file.getKey(), file.getVersionId());
          log.info("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5013 | DELETE-FILE-SUCCESS | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() +  " : " + file.getVersionId() + ")" + " ]");
        }
        catch(AmazonServiceException amazonServiceException)
        {
          log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5013 | DELETE-FILE-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() +  " : " + file.getVersionId() + ")" + " | "
              + amazonServiceException.getMessage() + " ]");
        }
        catch(SdkClientException sdkClientException)
        {
          log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5013 | DELETE-FILE-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() +  " : " + file.getVersionId() + ")" + " | "
              + sdkClientException.getMessage() + " ]");
        }
      }
    }
    else
    {
      return -1;
    }
    
    return 0;
  }

  //create a local exclude_from.txt and upload it to S3
  public static int createExclusionsFile(S3File s3Dir, List<String> exclusions, String archiveCacheDir)
  {
    String excludeFile = "";
    FileWriter writer = null;
    try
    {
      File cacheDir = new File(archiveCacheDir + File.separator + s3Dir.getKey());
      if (!cacheDir.exists()) cacheDir.mkdirs();
      excludeFile = cacheDir + File.separator + EXCLUDED_FILE;
      File localCacheExcludeFile = new File(excludeFile);
      if (localCacheExcludeFile.exists()) localCacheExcludeFile.delete();
      writer = new FileWriter(excludeFile, true);
      for (String filename : exclusions)
      {
        filename = filename.replace(" ", "*");
        filename = filename.replace("#", "*");
        
        int n =  filename.lastIndexOf('.');
        String extension = filename.substring(n + 1);
        if ( n > -1 && ( extension.indexOf('[') != -1 
            || extension.indexOf(']') != -1 ))
        {
          String name = filename.substring(0,  n);
          name = name.replace("[", "\\[");
          name = name.replace("]", "\\]");
          extension = extension.replace("[", "*");
          extension = extension.replace("]", "*");
          
          filename = name + "." + extension;
        }
        else
        {
          filename = filename.replace("[", "\\[");
          filename = filename.replace("]", "\\]");
        }
                        
        writer.write(filename + "\r\n");
      }
      writer.close();
      
      localCacheExcludeFile = new File(excludeFile);
      
      String key = s3Dir.getKey() + localCacheExcludeFile.getName();
      putFile(key, localCacheExcludeFile, s3Dir.getBucketName());
      
      if (localCacheExcludeFile.exists())
        localCacheExcludeFile.delete();
      
      log.info("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5007 | CREATE-EXCLUSION-SUCCESS | S3FileUtil | "
          + excludeFile + " | " + s3Dir.getKey() + "  ]");
    }
    catch(Exception e)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5007 | CREATE-EXCLUSION-FILE-ERROR | S3FileUtil | "
          + excludeFile + " | " + s3Dir.getKey() + " | " + e.getMessage() + " ]");
      return -1;
    }
    finally
    {
      try
      {
        if (writer != null)
          writer.close();
      }
      catch (IOException e)
      {
        log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5007 | CREATE-EXCLUSION-FILE-ERROR | S3FileUtil | "
            + excludeFile + " | " + s3Dir.getKey() + " | " + e.getMessage() + " ]");
        return -2;
      }
    }
    return 0;
  }
  
  public static boolean isExcluded (String fileName, ArrayList<String> exclusionList)
  {
    if (exclusionList == null)
      return false;

    for (String excludedFileName : exclusionList)
    {
     if (fileName.equals(excludedFileName))
       return true;
    }
    return false;
  }
  
  //return true if file.key() ends with /
  public static boolean isS3DirectoryFormat(S3File file)
  {
    if(getFileName(file).length() > 0)
      return false;
    else
      return true; //ends with "/"
  }
  
  //return filename of which file.getKey() does not end with /
  public static String getFileName(S3File file)
  {
    Matcher m = pattern.matcher(file.getKey());
    m.find();
    String lastToken = file.getKey().replace(m.group(), "");
    return lastToken;
  }
  
  //return filename of which key does not end with /
  public static String getFileName(String key)
  {
    Matcher m = pattern.matcher(key.trim());
    m.find();
    String lastToken = key.trim().replace(m.group(), "");
    return lastToken;
  }
  
  //return directory path ending with /
  public static String getParentPath(S3File file)
  {
    String fileName = getFileName(file);
    String parent = file.getKey().replace(fileName, "");
    return parent;
  }
  
  public static boolean isDirectory(S3File file)
  {
    return isDirectory(file.getKey(), file.getBucketName());
  }
  
  public static boolean isDirectory(String key, String bucketName)
  {
    String prefix = key;
    String delimiter = "/";
    if (!prefix.endsWith(delimiter))
    {
      prefix += delimiter;
    }

    try
    {
      log.debug("Listings Object:: bucket= " + bucketName + ", key= " + prefix);

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix)
          .withDelimiter(delimiter).withMaxKeys(1);
      ObjectListing list = s3Client.listObjects(listObjectsRequest);

      if (list.getCommonPrefixes().size() > 0) //if directory is empty, list.getCommonPrefixes() is []. If directory is not empty, list.getCommonPrefixes() is []. if it is file, list.getCommonPrefixes() is [].
        return true;

      for (S3ObjectSummary summary : list.getObjectSummaries()) //if it is file, list.getObjectSummaries() is [].
      {
        //if directory is not empty, for example, summary.getKey() has docfiles/dev4/prod4-qared/data-feed-export/prod4-qared_DocumentList_09-20-2017-22-00_01.xml
        if (summary.getKey().contains(prefix) && summary.getKey().indexOf(prefix) == 0) //if directory is empty, summary.getKey() has docfiles/dev4/prod4-qared/2001/
          return true;
      }
    }
    catch (AmazonServiceException amazonServiceException)
    {
      log.error(
          "[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5008 | IS-DIRECTORY-ERROR | S3FileUtil | "
              + "(" + bucketName + ":" + key + ")" + " | " + amazonServiceException.getMessage() + " ]");
    }
    catch(SdkClientException sce)
    {
      log.error(
          "[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() + " | ID-5008 | IS-DIRECTORY-ERROR | S3FileUtil | "
              + "(" + bucketName + ":" + key + ")" + " | " + sce.getMessage() + " ]");
    }

    return false;
  }
  
  public static boolean exists(S3File file)
  {
    return exists(file.getKey(), file.getBucketName());
  }
  
  public static boolean exists(String key , String bucketName)
  {
    boolean exists = false;
    try
    {
      log.debug("Object Exist:: bucket= " + bucketName + ", key= " + key);
  
      exists = s3Client.doesObjectExist(bucketName, key);
      
      if (!exists)
        exists = isDirectory(key, bucketName);
    }
    catch (AmazonServiceException amazonServiceException)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5009 | EXISTS-ERROR | S3FileUtil | " + "(" + bucketName + ":" + key + ")" + " | "
          + amazonServiceException.getMessage() + " ]");
    }
    catch(SdkClientException sce)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5009 | EXISTS-ERROR | S3FileUtil | " + "(" + bucketName + ":" + key + ")" + " | "
          + sce.getMessage() + " ]");
    }
    return exists;
  }
  
  public static S3File[] listFiles(S3File file)
  {
    ArrayList<S3File> keys = new ArrayList<S3File>();

    String key = file.getKey(); 
    try
    {
      log.debug("Listings Object:: bucket= " + file.getBucketName() + ", key= " + key);

      ObjectListing list = s3Client.listObjects(file.getBucketName(), key);
      for (S3ObjectSummary summary : list.getObjectSummaries())
      {
        if (!summary.getKey().equals(file.getKey()))
          keys.add(new S3File(summary.getKey(), summary.getBucketName())); //return only children, not itself.
      }
    }
    catch (AmazonServiceException amazonServiceException)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5010 | LIST-FILES-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() + ")" + " | "
          + amazonServiceException.getMessage() + " ]");
      return null;
    }
    catch(SdkClientException sce)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5010 | LIST-FILES-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() + ")" + " | "
          + sce.getMessage() + " ]");
      return null;
    }

    if (keys.size() > 0)
      return keys.toArray(new S3File[keys.size()]);
    else
      return null;
  }
  
  public static String[] listFileKeys(S3File file)
  {
    ArrayList<String> keys = new ArrayList<String>();

    String key = file.getKey();
    try
    {
      log.debug("Listings Object:: bucket= " + file.getBucketName() + ", key= " + key);

      ObjectListing list = s3Client.listObjects(file.getBucketName(), key);
      for (S3ObjectSummary summary : list.getObjectSummaries())
      {
        keys.add(summary.getKey());
      }
    }
    catch (AmazonServiceException e)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5011 | LIST-FILE-KEYS-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() + ")" + " | "
          + e.getMessage() + " ]");
      return null;
    }
    catch (SdkClientException sce)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5011 | LIST-FILE-KEYS-ERROR | S3FileUtil | " + "(" + file.getBucketName() + ":" + file.getKey() + ")" + " | "
          + sce.getMessage() + " ]");
      return null;
    }
    
    return keys.toArray(new String[keys.size()]);
  }
  
  public static Long getFileSize(S3File file)
  {
    return getFileSize(file.getKey(), file.getBucketName());
  }
  
  public static Long getFileSize(String key, String bucketName)
  {
    log.debug("Get Object Metadata:: bucket= " + bucketName + ", key= " + key);

    ObjectMetadata data = s3Client.getObjectMetadata(bucketName, key);
    
    if (data != null)
      return data.getContentLength();
    else
      return -1L;
  }
  
  //shutdown daemon threads
  public static void shutdownS3Daemons()
  {
    try
    {
      com.amazonaws.http.IdleConnectionReaper.shutdown();
      transferManager.shutdownNow(true);
    }
    catch(Exception e)
    {
      log.error("[ " + ServiceComponent.UNARCHIVE_S1SVC.getComponent() +" | ID-5012 | SHUTDOWN-S3-DAEMONS | S3FileUtil | " + " | "
          + e.getMessage() + " ]");
    }
  }
  
  public static void mkdirsS3(S3File file)
  {
    mkdirsS3(file.getKey(), file.getBucketName());
  }
  public static void mkdirsS3(String key, String bucketName)
  {
    if (!key.endsWith(File.separator))
      key = key  + File.separator;
   
    // create meta-data for your folder and set content-length to 0
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);

    // create empty content
    InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

    // create a PutObjectRequest passing the folder name suffixed by /
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, emptyContent, metadata);

    // send request to S3 to create folder
    s3Client.putObject(putObjectRequest);
  }
  
  public static boolean isEmptyDirectory(String key, String bucketName)
  {
    
    if (isDirectory(key, bucketName))
    {
      S3File[] files = listFiles(new S3File(key, bucketName));
      if (files == null || files.length == 0)
        return true;
    } 
    return false;
  }

  public static boolean isEmptyDirectory(S3File file)
  {
    if (isDirectory(file))
    {
      String key = file.getKey();
      if (!key.endsWith(File.separator))
        key = key + File.separator;
      S3File[] files = listFiles(file);
      if (files != null && files.length == 1)
      {
        S3File f = files[0];
        if (f.getKey().equals(key))
          return true;
      }
    } 
    return false;
  }
  
  public static S3File[] listVersions(S3File file)
  {
    ArrayList<S3File> keys = new ArrayList<S3File>();

    String key = file.getKey(); 
    try
    {
      log.debug("Listings Object:: bucket= " + file.getBucketName() + ", key= " + key);

      VersionListing version_listing = s3Client.listVersions(file.getBucketName(), file.getKey());
      
      while (true)
      {
        for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();)
        {
          S3VersionSummary vs = (S3VersionSummary)iterator.next();
          keys.add(new S3File(vs.getKey(), vs.getBucketName(), vs.getVersionId()));
        }
    
        if (version_listing.isTruncated())
        {
          System.out.println("version_listing.isTruncated() =================");
          version_listing = s3Client.listNextBatchOfVersions(version_listing);
        }
        else
        {
            break;
        }
      }
    }
    catch (AmazonServiceException e) 
    {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
    if (keys.size() > 0)
      return keys.toArray(new S3File[keys.size()]);
    else
      return null;
  }
}