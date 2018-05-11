package com.thomsonreuters.scholarone.unarchivefiles.audit;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AuditFileRevert
{
  Map<String, List<String>> sourceSnapshot = null;
  Map<String, List<String>> destinationSnapshot = null;

  public AuditFileRevert(Map<String, List<String>> sourceSnapshot, Map<String, List<String>> destinationSnapshot)
  {
    this.sourceSnapshot = sourceSnapshot;
    this.destinationSnapshot = destinationSnapshot;
  }

  public boolean areFilesReverted()
  {
    // fail if either is null
    if (sourceSnapshot == null || destinationSnapshot == null)
      return false;
    // iterate through source files and see if the files exist in destination
    // since paths are different, we'll check count of each file
    Iterator<Entry<String, List<String>>> it = sourceSnapshot.entrySet().iterator();
    while (it.hasNext())
    {
      Map.Entry<String, List<String>> pair = (Map.Entry<String, List<String>>) it.next();
      String fileName = pair.getKey();
      int fileCount = pair.getValue().size();
      if(!destinationSnapshot.containsKey(fileName))
        return false;
      if(fileCount > destinationSnapshot.get(fileName).size())
        return false;
    }

    return true;
  }

}
