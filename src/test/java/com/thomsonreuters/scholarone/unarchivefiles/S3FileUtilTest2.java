package com.thomsonreuters.scholarone.unarchivefiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.amazonaws.services.s3.model.StorageClass;
import com.scholarone.archivefiles.common.FileUtility;
import com.scholarone.archivefiles.common.S3File;
import com.scholarone.archivefiles.common.S3FileUtil;
import com.scholarone.monitoring.common.Environment;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class S3FileUtilTest2
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

  @Test
  public void testAStart() throws IOException
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
      f.delete();
    }
  }

  @Test
  public void testBCopy() throws IOException
  {
    StatInfo stat = new StatInfo();
    int exitCode = S3FileUtil.copyS3Dir(sourceS3Dir, destinationS3Dir, null, StorageClass.Standard, stat);
    Assert.assertTrue(exitCode == 0);

    S3File destFile1  = new S3File(destinationS3Dir.getKey() + "docfiles/_system_appendPDF_proof_hi.pdf",   destinationS3Dir.getBucketName() );
    S3File destFile2  = new S3File(destinationS3Dir.getKey() + "docfiles/_system_appendPDF_proof_output_test_only.pdf",   destinationS3Dir.getBucketName() );
    S3File destFile3  = new S3File(destinationS3Dir.getKey() + "docfiles/1-s2.0-S0144861713011223-main[paper1].pdf",   destinationS3Dir.getBucketName() );
    S3File destFile4  = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/FINAL-tropical- after second modification 15-7-2012[1].doc",   destinationS3Dir.getBucketName() );
    S3File destFile5  = new S3File(destinationS3Dir.getKey() + "docfiles/notes/345206/[Linear Algebra re 10,4. Welles. 7,1. 7,1. 2015.] (Figures).pdf",   destinationS3Dir.getBucketName() );
    S3File destFile6  = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/- nl-test.pdf",   destinationS3Dir.getBucketName() );
    S3File destFile7  = new S3File(destinationS3Dir.getKey() + "docfiles/original-files/502676_File000003_4095174.0]",   destinationS3Dir.getBucketName() );
    S3File destFile8  = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/#2_Second response to reviewers(JSLee).doc",   destinationS3Dir.getBucketName() );
    S3File destFile9  = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/jz201001661U-Reviewer#4.pdf",   destinationS3Dir.getBucketName() );
    S3File destFile10 = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/#7bis.tex",   destinationS3Dir.getBucketName() );
    S3File destFile11 = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/\" jm-2012-01013n.R1.pdf",   destinationS3Dir.getBucketName() );
    S3File destFile12 = new S3File(destinationS3Dir.getKey() + "docfiles/notes/5455/$7M to spend 8-18[1]-Bradley.docx",   destinationS3Dir.getBucketName() );
    S3File destFile13 = new S3File(destinationS3Dir.getKey() + "docfiles/tex/home/rbb/sources/LaTeX/bibtex/bst/nf.bst",   destinationS3Dir.getBucketName() );

    Assert.assertTrue(S3FileUtil.exists(destFile1 ));
    Assert.assertTrue(S3FileUtil.exists(destFile2 ));
    Assert.assertTrue(S3FileUtil.exists(destFile3 ));
    Assert.assertTrue(S3FileUtil.exists(destFile4 ));
    Assert.assertTrue(S3FileUtil.exists(destFile5 ));
    Assert.assertTrue(S3FileUtil.exists(destFile6 ));
    Assert.assertTrue(S3FileUtil.exists(destFile7 ));
    Assert.assertTrue(S3FileUtil.exists(destFile8 ));
    Assert.assertTrue(S3FileUtil.exists(destFile9 ));
    Assert.assertTrue(S3FileUtil.exists(destFile10));
    Assert.assertTrue(S3FileUtil.exists(destFile11));
    Assert.assertTrue(S3FileUtil.exists(destFile12));
    Assert.assertTrue(S3FileUtil.exists(destFile13));
  }
  

  @Test
  public void testCEnd() throws IOException
  {
    File lockFile = new File(sourceDir.getPath() + File.separator + TaskLock.LOCK);
    if (lockFile.exists())
      lockFile.delete();
    
    if (S3FileUtil.isDirectory(sourceS3Dir))
      S3FileUtil.deleteS3AllVersionsRecursive(sourceS3Dir, null);
    
    if (S3FileUtil.isDirectory(destinationS3Dir))
      S3FileUtil.deleteS3AllVersionsRecursive(destinationS3Dir, null);
  }
}
