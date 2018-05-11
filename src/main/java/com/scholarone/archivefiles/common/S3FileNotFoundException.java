package com.scholarone.archivefiles.common;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

public class S3FileNotFoundException extends IOException
{
  private static final long serialVersionUID = -36619241610511376L;

  private Throwable cause;
  
  public S3FileNotFoundException()
  {
    super();
  };
  
  public S3FileNotFoundException(String msg)
  {
    super(msg);
  }
  
  public S3FileNotFoundException(Throwable cause)
  {
    this.cause = cause;
  }

  public Throwable getCause()
  {
    return cause;
  }

  public void setCause(Throwable cause)
  {
    this.cause = cause;
  }
  
  @Override
  public String getMessage()
  {
    if (super.getMessage() != null)
      return super.getMessage();
    else
      return "";
  }
 
  public void printStackTrace()
  {
    if (cause != null)
      cause.printStackTrace();
    else
      super.printStackTrace();
  }
  
  public void printStackTrace(PrintStream s)
  {
    if (cause != null)
      cause.printStackTrace(s);
    else
      super.printStackTrace(s);
  }

  public void printStackTrace(PrintWriter s)
  {
    if (cause != null)
      cause.printStackTrace(s);
    else
      super.printStackTrace(s);
  }
}
