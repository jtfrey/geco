/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOLog.c
 *
 *  Logging functionality.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOLog.h"

#include <syslog.h>

//

const char*           __GECOLogLabels[] = {
                          "EMERG",
                          "",
                          "ERROR",
                          "WARN ",
                          "INFO ",
                          "DEBUG"
                        };
                        
const int             __GECOLogSyslogMap[] = {
                          LOG_ALERT,
                          0,
                          LOG_ERR,
                          LOG_WARNING,
                          LOG_INFO,
                          LOG_DEBUG
                        };

//

static GECOLogRef GECOLogDefault = NULL;

//

GECOLogRef
GECOLogGetDefault(void)
{
  return GECOLogDefault;
}

//

GECOLogRef
GECOLogSetDefault(
  GECOLogRef    theLog
)
{
  GECOLogRef    oldLog = GECOLogDefault;
  
  GECOLogDefault = theLog;
  return oldLog;
}

//

typedef struct _GECOLog {
  FILE          *logFPtr;
  GECOLogLevel  logLevel;
  GECOLogFormat logFormat;
  bool          shouldCloseWhenDone, isConstant;
} GECOLog;

GECOLog*
__GECOLogAlloc(void)
{
  GECOLog   *newLog = malloc(sizeof(GECOLog));
  
  if ( newLog ) {
    newLog->logFPtr             = NULL;
    newLog->logLevel            = GECOLogLevelDefault;
    newLog->logFormat           = GECOLogFormatDefault;
    newLog->shouldCloseWhenDone = true;
    newLog->isConstant          = false;
  }
  return newLog;
}

//

GECOLogRef
GECOLogCreateWithFilePath(
  GECOLogLevel    logLevel,
  const char      *filePath
)
{
  GECOLogRef      newLog = NULL;
  FILE            *logFPtr = fopen(filePath, "a");
  
  if ( logFPtr ) {
    newLog = GECOLogCreateWithFilePointer(logLevel, logFPtr, true);
    if ( ! newLog ) fclose(logFPtr);
  }
  return newLog;
}

//

GECOLogRef
GECOLogCreateWithFilePointer(
  GECOLogLevel    logLevel,
  FILE            *filePtr,
  bool            shouldCloseWhenDone
)
{
  GECOLog         *newLog = __GECOLogAlloc();
  
  if ( newLog ) {
    newLog->logFPtr = filePtr;
    newLog->logLevel = logLevel;
    newLog->shouldCloseWhenDone = shouldCloseWhenDone;
    setvbuf(filePtr, NULL, _IOLBF, 0);
  }
  return newLog;
}

//

GECOLogRef
GECOLogSharedDefault(void)
{
  static GECOLogRef GECOLogDefaultInstance = NULL;
  
  if ( ! GECOLogDefaultInstance ) {
    GECOLogDefaultInstance = __GECOLogAlloc();
    if ( GECOLogDefaultInstance ) {
      GECOLogDefaultInstance->logFPtr = stderr;
      GECOLogDefaultInstance->shouldCloseWhenDone = false;
      GECOLogDefaultInstance->isConstant = true;
      setvbuf(stderr, NULL, _IOLBF, 0);
    }
  }
  return GECOLogDefaultInstance;
}

//

void
GECOLogDestroy(
  GECOLogRef  theLog
)
{
  if ( theLog && ! theLog->isConstant ) {
    if ( theLog->logFPtr && theLog->shouldCloseWhenDone ) fclose(theLog->logFPtr);
    free((void*)theLog);
  }
}

//

GECOLogLevel
GECOLogGetLevel(
  GECOLogRef  theLog
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  return theLog->logLevel;
}

//

GECOLogLevel
GECOLogSetLevel(
  GECOLogRef    theLog,
  GECOLogLevel  theLogLevel
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  if ( theLogLevel >= GECOLogLevelQuiet && theLogLevel <= GECOLogLevelDebug ) theLog->logLevel = theLogLevel;
  return theLog->logLevel;
}

//


GECOLogLevel
GECOLogIncLevel(
  GECOLogRef  theLog
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  if ( theLog->logLevel++ == GECOLogLevelDebug ) theLog->logLevel = GECOLogLevelDebug;
  return theLog->logLevel;
}

//


GECOLogLevel
GECOLogDecLevel(
  GECOLogRef  theLog
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  if ( theLog->logLevel-- == GECOLogLevelQuiet ) theLog->logLevel = GECOLogLevelQuiet;
  return theLog->logLevel;
}

//

GECOLogFormat
GECOLogGetFormat(
  GECOLogRef  theLog
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  return theLog->logFormat;
}

//

void
GECOLogSetFormat(
  GECOLogRef    theLog,
  GECOLogFormat theFormat
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  theLog->logFormat = theFormat;
}

//

void
GECOLogVPrintf(
  GECOLogRef      theLog,
  GECOLogLevel    logAtLevel,
  const char      *format,
  va_list         argv
)
{
  if ( ! theLog ) theLog = GECOLogSharedDefault();
  
  if ( ((logAtLevel > GECOLogLevelQuiet) && logAtLevel <= theLog->logLevel) || (logAtLevel == GECOLogLevelEmergency) ) { 
    char          timestamp[32];
    
    if ( (logAtLevel == GECOLogLevelEmergency) || (theLog->logFormat & GECOLogFormatSyslog) == GECOLogFormatSyslog ) {
      va_list     syslogArgv;
      
      va_copy(syslogArgv, argv);
      vsyslog(__GECOLogSyslogMap[logAtLevel + 1] , format, syslogArgv);
    }
    
    if ( (theLog->logFormat & GECOLogFormatTimestamp) == GECOLogFormatTimestamp ) {
      struct tm   dateAndTime;
      time_t      now = time(NULL);
      
      localtime_r(&now, &dateAndTime);
      strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S%z", &dateAndTime);
    }
    switch ( theLog->logFormat & (GECOLogFormatTimestamp | GECOLogFormatPid | GECOLogFormatLevelLabel) ) {
    
      case (GECOLogFormatTimestamp | GECOLogFormatPid | GECOLogFormatLevelLabel):
        fprintf(theLog->logFPtr, "%s [%d|%s]:", timestamp, (int)getpid(), __GECOLogLabels[logAtLevel + 1]);
        break;
    
      case (GECOLogFormatTimestamp | GECOLogFormatPid):
        fprintf(theLog->logFPtr, "%s [%d]:", timestamp, (int)getpid(), __GECOLogLabels[logAtLevel + 1]);
        break;
    
      case (GECOLogFormatTimestamp | GECOLogFormatLevelLabel):
        fprintf(theLog->logFPtr, "%s [%s]:", timestamp, __GECOLogLabels[logAtLevel + 1]);
        break;
    
      case (GECOLogFormatPid | GECOLogFormatLevelLabel):
        fprintf(theLog->logFPtr, "[%d|%s]:", (int)getpid(), __GECOLogLabels[logAtLevel + 1]);
        break;
    
      case GECOLogFormatTimestamp:
        fprintf(theLog->logFPtr, "%s:", timestamp);
        break;
    
      case GECOLogFormatPid:
        fprintf(theLog->logFPtr, "[%d]:", (int)getpid());
        break;
    
      case GECOLogFormatLevelLabel:
        fprintf(theLog->logFPtr, "[%s]:", __GECOLogLabels[logAtLevel + 1]);
        break;
    
    }
    vfprintf(theLog->logFPtr, format, argv);
    
    fputc('\n', theLog->logFPtr);
  }
  if ( logAtLevel == GECOLogLevelEmergency ) exit(1);
}

//

void
GECOLogPrintf(
  GECOLogRef      theLog,
  GECOLogLevel    logAtLevel,
  const char      *format,
  ...
)
{
  va_list         argv;
  
  va_start(argv, format);
  GECOLogVPrintf(theLog, logAtLevel, format, argv);
  va_end(argv);
}
