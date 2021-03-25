/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOLog.h
 *
 *  Logging functionality.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOLOG_H__
#define __GECOLOG_H__

#include "GECO.h"

typedef enum {
  GECOLogLevelEmergency   = -1,
  //
  GECOLogLevelQuiet = 0,
  GECOLogLevelError = 1,
  GECOLogLevelWarn  = 2,
  GECOLogLevelInfo  = 3,
  GECOLogLevelDebug = 4,
  //
  GECOLogLevelDefault = GECOLogLevelError
} GECOLogLevel;

typedef struct _GECOLog * GECOLogRef;

GECOLogRef GECOLogGetDefault(void);
GECOLogRef GECOLogSetDefault(GECOLogRef theLog);

GECOLogRef GECOLogCreateWithFilePath(GECOLogLevel logLevel, const char *filePath);
GECOLogRef GECOLogCreateWithFilePointer(GECOLogLevel logLevel, FILE *filePtr, bool shouldCloseWhenDone);
GECOLogRef GECOLogSharedDefault(void);
void GECOLogDestroy(GECOLogRef theLog);

GECOLogLevel GECOLogGetLevel(GECOLogRef theLog);
GECOLogLevel GECOLogSetLevel(GECOLogRef theLog, GECOLogLevel theLogLevel);
GECOLogLevel GECOLogIncLevel(GECOLogRef theLog);
GECOLogLevel GECOLogDecLevel(GECOLogRef theLog);

typedef enum {
  GECOLogFormatTimestamp      = 1,
  GECOLogFormatPid            = 2,
  GECOLogFormatLevelLabel     = 4,
  GECOLogFormatSyslog         = 8,
  //
  GECOLogFormatDefault        = GECOLogFormatTimestamp | GECOLogFormatPid | GECOLogFormatLevelLabel
} GECOLogFormat;

GECOLogFormat GECOLogGetFormat(GECOLogRef theLog);
void GECOLogSetFormat(GECOLogRef theLog, GECOLogFormat theFormat);

void GECOLogPrintf(GECOLogRef theLog, GECOLogLevel logAtLevel, const char *format, ...);
void GECOLogVPrintf(GECOLogRef theLog, GECOLogLevel logAtLevel, const char *format, va_list argv);

#ifdef GECO_DEBUG_DISABLE
#define GECO_DEBUG_F(GECO_LOG_OBJ, GECO_FORMAT, ...)
#else
#define GECO_DEBUG_F(GECO_LOG_OBJ, GECO_FORMAT, ...) { GECOLogPrintf(GECO_LOG_OBJ, GECOLogLevelDebug, "(%s:%d) "GECO_FORMAT, __FILE__, __LINE__, ##__VA_ARGS__); }
#endif

#ifdef GECO_INFO_DISABLE
#define GECO_INFO_F(GECO_LOG_OBJ, GECO_FORMAT, ...)
#else
#define GECO_INFO_F(GECO_LOG_OBJ, GECO_FORMAT, ...) { GECOLogPrintf(GECO_LOG_OBJ, GECOLogLevelInfo, "(%s:%d) "GECO_FORMAT, __FILE__, __LINE__, ##__VA_ARGS__); }
#endif

#define GECO_WARN_F(GECO_LOG_OBJ, ...) { GECOLogPrintf(GECO_LOG_OBJ, GECOLogLevelWarn, __VA_ARGS__); }
#define GECO_ERROR_F(GECO_LOG_OBJ, ...) { GECOLogPrintf(GECO_LOG_OBJ, GECOLogLevelError, __VA_ARGS__); }
#define GECO_EMERGENCY_F(GECO_LOG_OBJ, ...) { GECOLogPrintf(GECO_LOG_OBJ, GECOLogLevelEmergency, __VA_ARGS__); }


#define GECO_DEBUG(GECO_FORMAT, ...) GECO_DEBUG_F(GECOLogGetDefault(), GECO_FORMAT, ##__VA_ARGS__)
#define GECO_INFO(GECO_FORMAT, ...) GECO_INFO_F(GECOLogGetDefault(), GECO_FORMAT, ##__VA_ARGS__)
#define GECO_WARN(...) GECO_WARN_F(GECOLogGetDefault(), __VA_ARGS__)
#define GECO_ERROR(...) GECO_ERROR_F(GECOLogGetDefault(), __VA_ARGS__)
#define GECO_EMERGENCY(...) GECO_EMERGENCY_F(GECOLogGetDefault(), __VA_ARGS__)

#endif /* __GECOLOG_H__ */
