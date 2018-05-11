package com.thomsonreuters.scholarone.unarchivefiles;

import java.text.DecimalFormat;

import com.scholarone.activitytracker.TrackingInfo;

public class StatInfo extends TrackingInfo
{
  private Integer numOfFilesTotal = Integer.valueOf(0);

  private Integer numOfFilesSuccess = Integer.valueOf(0);
  
  private Integer numOfFilesFailure = Integer.valueOf(0);
  
  public StatInfo ()
  {
    super();
  }
  
  public StatInfo (String message)
  {
    super(message);
  }
  
  public Integer getNumOfFilesTotal()
  {
    return numOfFilesTotal;
  }
  public void setNumOfFilesTotal(Integer numOfFilesTotal)
  {
    this.numOfFilesTotal = numOfFilesTotal;
  }
  public Integer getNumOfFilesSuccess()
  {
    return numOfFilesSuccess;
  }
  public void setNumOfFilesSuccess(Integer numOfFilesSuccess)
  {
    this.numOfFilesSuccess = numOfFilesSuccess;
  }
  public Integer getNumOfFilesFailure()
  {
    return numOfFilesFailure;
  }
  public void setNumOfFilesFailure(Integer numOfFilesFailure)
  {
    this.numOfFilesFailure = numOfFilesFailure;
  }
  
  public void decreaseNumOfFilesTotal()
  {
    this.numOfFilesTotal--;
  }
  public void increaseNumOfFilesSuccess()
  {
    this.numOfFilesSuccess++;
  }
  public void increaseNumOfFilesFailure()
  {
    this.numOfFilesFailure++;
  }

  @Override
  public Double getTransferRate()
  {
    if (super.getTransferRate() == null)
      return 0.0;
    else
      return super.getTransferRate();
  }

  @Override
  public Long getTransferSize()
  {
    if (super.getTransferSize() == null)
      return 0L;
    else
      return super.getTransferSize();
  }

  @Override
  public Long getTransferTime()
  {
    if (super.getTransferTime() == null)
      return 0L;
    else
      return super.getTransferTime();
  }

  public String toString()
  {
    DecimalFormat formatter = new DecimalFormat("#0");
    
    StringBuilder sb = new StringBuilder();
    sb.append("environment=" + super.getEnvironment())
      .append(", stackId=" + super.getStackId())
      .append(", message=" + super.getMessage())
      .append(", groupId=" + super.getGroupId())
      .append(", name=" + super.getName())
      .append(", objectTypeId=" + super.getObjectTypeId())
      .append(", objectId=" + super.getObjectId())
      .append(", type=" + super.getType())
      .append(", successCount=" + super.getSuccessCount())
      .append(", failureCount=" + super.getFailureCount())
      .append(", pendingCount=" + super.getPendingCount())
      .append(", onHoldCount=" + super.getOnHoldCount())
      .append(", totalCount=" + super.getTotalCount())
      .append(", numOfFilesSuccess=" + this.getNumOfFilesSuccess())
      .append(", numOfFilesFailure=" + this.getNumOfFilesFailure())
      .append(", numOfFilesTotal ="  + ((int) this.numOfFilesSuccess + (int) this.numOfFilesFailure))
      .append(", transferSize=" + super.getTransferSize() + " bytes")
      .append(", transferTime=" + super.getTransferTime() + " ms")
      .append((super.getTransferRate() == null ? "" : ", transferRate=" + formatter.format(super.getTransferRate()) + " bytes/ ms"))
      .append(", startTime=" + super.getStartTime())
      .append(", endTime=" + super.getEndTime());
    return sb.toString();
  }

}
