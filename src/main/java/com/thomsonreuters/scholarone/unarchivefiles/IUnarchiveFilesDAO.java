package com.thomsonreuters.scholarone.unarchivefiles;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public interface IUnarchiveFilesDAO
{
  public abstract boolean openConnection(Integer stackId);

  public abstract void closeConnection();

  public abstract List<Config> getConfigs() throws SQLException;

  public abstract List<Document> getDocuments(Integer configId, Integer rows) throws SQLException;
  
  public abstract boolean updateDocument(Integer documentId, Integer retryCount, boolean filesCopySuccess) throws SQLException;
}