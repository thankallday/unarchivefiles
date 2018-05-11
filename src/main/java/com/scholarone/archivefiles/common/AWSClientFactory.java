package com.scholarone.archivefiles.common;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.thomsonreuters.scholarone.unarchivefiles.ConfigPropertyValues;


public class AWSClientFactory
{
  private static AWSClientFactory instance = null;

  private static AmazonS3 s3Client;

  private static TransferManager transferManager;

  private static ClientConfiguration clientConfig;

  private AWSClientFactory()
  {
  }

  public static AWSClientFactory getInstance()
  {
    if (instance == null)
    {
      instance = new AWSClientFactory();
      instance.init();
    }

    return instance;
  }

  private void init()
  {
    clientConfig = new ClientConfiguration();
    String proxyHost;

	proxyHost = ConfigPropertyValues.getProperty("PROXY_HOST");
	if (proxyHost != null && proxyHost.length() > 0)
	{
	  clientConfig.setProxyHost(proxyHost);
	  clientConfig.setProxyPort(Integer.valueOf(ConfigPropertyValues.getProperty("PROXY_PORT")));
	}
  }

  public AmazonS3 getS3Client()
  {
    if (s3Client == null)
    {
      s3Client = AmazonS3ClientBuilder.standard()
		      .withRegion(ConfigPropertyValues.getProperty("aws.region"))
		      .withClientConfiguration(clientConfig).build();
    }
    return s3Client;
  }

  public TransferManager getTransferManager()
  {
    if (transferManager == null)
    {
      transferManager = TransferManagerBuilder.standard().withExecutorFactory(new S3ExecutorFactory())
          .withMinimumUploadPartSize(100L).withS3Client(s3Client).build();
    }

    return transferManager;
  }
}
