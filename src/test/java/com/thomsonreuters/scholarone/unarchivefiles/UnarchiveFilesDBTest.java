package com.thomsonreuters.scholarone.unarchivefiles;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnarchiveFilesDBTest
{

  @Test
  public void testAGetConfigs()
  {
    IUnarchiveFilesDAO db = new UnarchiveFilesDAOTestImpl();

    db.openConnection(4);
    List<Config> configs = new ArrayList<Config>();
    try
    {
      configs = db.getConfigs();
    }
    catch (SQLException e)
    {
      fail(e.getMessage());
    }
    db.closeConnection();

    assertTrue(configs.size() > 0);
  }

  @Test
  public void testBGetDocuments()
  {
    UnarchiveFilesDAOTestImpl db = new UnarchiveFilesDAOTestImpl();

    db.openConnection(4);
    List<Document> documents = new ArrayList<Document>();

    db.deleteDocument();
    db.addDocument();

    try
    {
      documents = db.getDocuments(534, 100);
    }
    catch (SQLException e)
    {
      fail(e.getMessage());
    }

    db.deleteDocument();
    db.closeConnection();

    assertTrue(documents.size() > 0);
  }

  @Test
  public void testCUpdateDocument()
  {
    UnarchiveFilesDAOTestImpl db = new UnarchiveFilesDAOTestImpl();

    db.openConnection(4);

    // Setup
    db.deleteDocument();
    db.addDocument();

    boolean rv = false;
    try
    {
      rv = db.updateDocument(0, 0, true);
      rv = db.updateDocument(0, 0, false);
    }
    catch (SQLException e)
    {
      fail(e.getMessage());
    }

    db.deleteDocument();

    db.closeConnection();

    assertTrue(rv);
  }
}
