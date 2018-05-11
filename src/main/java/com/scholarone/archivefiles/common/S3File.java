package com.scholarone.archivefiles.common;

import org.apache.log4j.Logger;

public class S3File
{
  protected static Logger log = Logger.getLogger(S3File.class.getName());

  private String key; //For directory, it ends with /. For example, docfiles/dev1/qared/2018/03/1234/
                      //For file, it does not end with /. For example, docfiles/dev1/qared/2018/03/1234/original-files/abc.docx
  private String bucketName;
  
  private String versionId;
  
  public S3File(String key, String bucketName)
  {
    this.key = key;
    this.bucketName = bucketName;
    this.versionId = null;
  }
  
  public S3File(String key, String bucketName, String versionId)
  {
    this(key, bucketName);
    this.versionId = versionId;    
  }
  
  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }
  
  public String getVersionId()
  {
    return versionId;
  }

  public void setVersionId(String versionId)
  {
    this.versionId = versionId;
  }
  
  public String getBucketName()
  {
    return bucketName;
  }

  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  public void setBucket(String bucketName)
  {
    this.bucketName = bucketName;
  }
  
  public String toString()
  {
    return this.getKey() + " - " + this.getBucketName();
  }

  public S3File[] listFiles()
  {
    return S3FileUtil.listFiles(this);
  }
  
  public String[] listFileKeys()
  {
    return S3FileUtil.listFileKeys(this);
  } 
  
  
  public S3File[] listVersions()
  {
    return S3FileUtil.listVersions(this);
  }
}
