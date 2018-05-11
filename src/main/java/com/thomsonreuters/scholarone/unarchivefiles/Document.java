package com.thomsonreuters.scholarone.unarchivefiles;

import java.sql.Timestamp;


public class Document
{
  Integer documentId;
  
  Integer fileStoreMonth;
  
  Integer fileStoreYear;
  
  Integer archiveMonth;
  
  Integer archiveYear;
  
  Integer retryCount = Integer.valueOf(0);
  
  Timestamp datetimeFilesMoved;
  
  public Timestamp getDatetimeFilesMoved()
  {
    return datetimeFilesMoved;
  }

  public void setDatetimeFilesMoved(Timestamp datetimeFilesMoved)
  {
    this.datetimeFilesMoved = datetimeFilesMoved;
  }

  public Integer getDocumentId()
  {
    return documentId;
  }

  public void setDocumentId(Integer documentId)
  {
    this.documentId = documentId;
  }
  
  public String getArchiveMonth()
  {
    String month = archiveMonth.toString();
    if (month.length() == 1)
    {
      month = "0" + month;
    }
    
    return month;
  }

  public void setArchiveMonth(Integer archiveMonth)
  {
    this.archiveMonth = archiveMonth;
  }

  public String getArchiveYear()
  {
    return archiveYear.toString();
  }

  public void setArchiveYear(Integer archiveYear)
  {
    this.archiveYear = archiveYear;
  }
  
  public String getFileStoreMonth()
  {
    String month = fileStoreMonth.toString();
    if (month.length() == 1)
    {
      month = "0" + month;
    }
    
    return month;
  }

  public void setFileStoreMonth(Integer fileStoreMonth)
  {
    this.fileStoreMonth = fileStoreMonth;
  }

  public String getFileStoreYear()
  {
    return fileStoreYear.toString();
  }

  public void setFileStoreYear(Integer fileStoreYear)
  {
    this.fileStoreYear = fileStoreYear;
  }

  public Integer getRetryCount()
  {
    return retryCount;
  }

  public void setRetryCount(Integer retryCount)
  {
    this.retryCount = retryCount;
  }
  
  public void incrementRetryCount()
  {
    this.retryCount++;
  }
}
