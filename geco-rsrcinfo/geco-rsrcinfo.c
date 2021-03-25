/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  geco-rsrc-info.c
 *  
 *  Standalone program that uses the GECOResource code to display
 *  summaries of resource allocations.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOResource.h"
#include "GECOCGroup.h"
#include "GECOLog.h"
#include <getopt.h>

const struct option geco_cli_options[] = {
                  { "help",                 no_argument,          NULL,         'h' },
                  { "verbose",              no_argument,          NULL,         'v' },
                  { "quiet",                no_argument,          NULL,         'q' },
                  { "mode",                 required_argument,    NULL,         'm' },
                  { "prolog",               no_argument,          NULL,         'p' },
                  { "epilog",               no_argument,          NULL,         'e' },
                  { "only",                 no_argument,          NULL,         'o' },
                  { "host",                 required_argument,    NULL,         'H' },
                  { "jobid",                required_argument,    NULL,         'j' },
                  { "serialize",            required_argument,    NULL,         's' },
                  { "unserialize",          required_argument,    NULL,         'u' },
                  { "qstat-retry",          required_argument,    NULL,         'r' },
                  { NULL,                   0,                    0,             0  }
                };

//

void
usage(
  const char    *exe
)
{
  printf(
      "usage:\n\n"
      "  %s {options} [task-id]\n\n"
      " options:\n\n"
      "  -h/--help                    show this information\n"
      "  -v/--verbose                 increase the verbosity level (may be used\n"
      "                                 multiple times)\n"
      "  -q/--quiet                   decrease the verbosity level (may be used\n"
      "                                 multiple times)\n"
      "  -m/--mode=[mode]             operate in the given mode:\n"
      "                                 prolog:   SGE prolog script\n"
      "                                 epilog:   SGE epilog script\n"
      "                                 userenv:  user environment\n"
      "  -p/--prolog                  shorthand for --mode=prolog\n"
      "  -e/--epilog                  shorthand for --mode=epilog\n"
      "  -o/--only                    return information for the native\n"
      "                               host only, not an array of hosts\n"
      "  -H/--host=[hostname]         return information for the specified\n"
      "                               host only, not an array of hosts\n"
      "  -j/--jobid=[job_id]          request info for a specific job id\n"
      "                                 (without this option, qstat output\n"
      "                                 is expected on stdin)\n"
      "  -s/--serialize=[path]        rather than displaying to stdout, serialize\n"
      "                                 the resource information to the given\n"
      "                                 filepath\n"
      "  -u/--unserialize=[path]      unserialize resource information in the\n"
      "                                 given filepath and display it\n"
      "  -r/--qstat-retry=#           if qstat fails to return data for a job, retry\n"
      "                                 this many times\n"
      "\n"
      " $Id$\n"
      "\n"
      ,
      exe
    );
}

//
////
//

int
main(
  int         argc,
  char        **argv
)
{
  const char                  *exe = argv[0];
  int                         optch;
  
  int                         rc = 0;
  
  long int                    jobId = -1;
  long int                    taskId = 1;
  char                        thisHost[64];
  bool                        thisHostOnly = false;
  const char                  *serializeToPath = NULL;
  const char                  *unserializeFromPath = NULL;
  int                         qstatRetryCount = 2;
  
  GECOResourceSetRef          theResources = NULL;
  GECOResourceSetExportMode   exportMode = GECOResourceSetExportModeUserEnv;
  
  // Init libxml:
  xmlInitParser();
  LIBXML_TEST_VERSION
  
  // Check for arguments:
  while ( (optch = getopt_long(argc, argv, "hvqm:peoH:j:s:u:r:", geco_cli_options, NULL)) != -1 ) {
    switch ( optch ) {
      
      case 'h':
        usage(exe);
        exit(0);
      
      case 'v':
        GECOLogIncLevel(GECOLogGetDefault());
        break;
      
      case 'q':
        GECOLogDecLevel(GECOLogGetDefault());
        break;
      
      case 'm': {
        if ( optarg && *optarg ) {
          if ( strcmp(optarg, "userenv") == 0 ) {
            exportMode = GECOResourceSetExportModeUserEnv;
          }
          else if ( strcmp(optarg, "prolog") == 0 ) {
            exportMode = GECOResourceSetExportModeGEProlog;
          }
          else if ( strcmp(optarg, "epilog") == 0 ) {
            exportMode = GECOResourceSetExportModeGEEpilog;
          }
          else {
            fprintf(stderr, "ERROR:  invalid operating mode:  %s\n", optarg);
            exit(EINVAL);
          }
        } else {
          fprintf(stderr, "ERROR:  no operating mode provided\n");
          exit(EINVAL);
        }
        break;
      }
      
      case 'p':
        exportMode = GECOResourceSetExportModeGEProlog;
        break;
      
      case 'e':
        exportMode = GECOResourceSetExportModeGEEpilog;
        break;
      
      case 'o':
        if ( gethostname(thisHost, sizeof(thisHost)) == -1 ) {
          fprintf(stderr, "ERROR:  failure in gethostname(): %d\n", errno);
          exit(errno);
        }
        thisHostOnly = true;
        break;
      
      case 'H':
        if ( optarg && *optarg ) {
          strncpy(thisHost, optarg, sizeof(thisHost));
        } else {
          fprintf(stderr, "ERROR:  no hostname provided\n");
          exit(EINVAL);
        }
        break;
      
      case 'j':
        if ( optarg && *optarg ) {
          const char    *endPtr;
          if ( ! GECO_strtol(optarg, &jobId, &endPtr) ) {
            fprintf(stderr, "ERROR:  invalid job id provided:  %s\n", argv[optind]);
            return EINVAL;
          }
          if ( *endPtr == '.' ) {
            if ( ! GECO_strtol(++endPtr, &taskId, &endPtr) ) {
              fprintf(stderr, "ERROR:  invalid task id provided:  %s\n", argv[optind]);
              return EINVAL;
            }
          }
        } else {
          fprintf(stderr, "ERROR:  no job id provided\n");
          exit(EINVAL);
        }
        break;
      
      case 's':
        if ( optarg && *optarg ) {
          serializeToPath = optarg;
        } else {
          fprintf(stderr, "ERROR:  no filepath provided to -s/--serialize\n");
          exit(EINVAL);
        }
        break;
      
      case 'u':
        if ( optarg && *optarg ) {
          unserializeFromPath = optarg;
        } else {
          fprintf(stderr, "ERROR:  no filepath provided to -u/--unserialize\n");
          exit(EINVAL);
        }
        break;
        
      case 'r': {
        long int      value;
        
        if ( optarg && *optarg && GECO_strtol(optarg, &value, NULL) ) {
          qstatRetryCount = value;
        } else {
          fprintf(stderr, "ERROR:  invalid or no value provided with -r/--qstat-retry\n");
          exit(EINVAL);
        }
        break;
      }

    }
  }
  if ( optind < argc ) {
    if ( strcmp(argv[optind], "undefined") && ! GECO_strtol(argv[optind], &taskId, NULL) ) {
      fprintf(stderr, "ERROR:  invalid task id provided:  %s\n", argv[optind]);
      return EINVAL;
    }
  }
  
  if ( unserializeFromPath ) {
    theResources = GECOResourceSetDeserialize(unserializeFromPath);
    if ( ! theResources ) {
      fprintf(stderr, "ERROR:  unable to unserialize data in %s (errno = %d)\n", unserializeFromPath, errno);
      return errno;
    }
  } else {
    GECOResourceSetCreateFailure    failureReason;
    
    // If a job id was provided, attempt to request the qstat output:
    if ( jobId >= 0 ) {
      theResources = GECOResourceSetCreate(jobId, taskId, qstatRetryCount, &failureReason);
    } else {
      theResources = GECOResourceSetCreateWithFileDescriptor(STDIN_FILENO, jobId, taskId, &failureReason);
    }
    
    // Anything wrong?
    switch ( failureReason ) {

      case GECOResourceSetCreateFailureCheckErrno: {
        rc = errno;
        fprintf(stderr, "ERROR: failed to find resource information (errno = %d)\n", errno);
        break;
      }

      case GECOResourceSetCreateFailureQstatFailure: {
        fprintf(stderr, "ERROR: failed to find resource information, general qstat failure\n");
        rc = EIO;
        break;
      }

      case GECOResourceSetCreateFailureMalformedQstatXML: {
        fprintf(stderr, "ERROR: failed to find resource information, qstat output is malformed\n");
        rc = EINVAL;
        break;
      }

      case GECOResourceSetCreateFailureJobDoesNotExist: {
        fprintf(stderr, "ERROR: job %ld.%ld is not known to the qmaster\n", jobId, taskId);
        rc = ENOENT;
        break;
      }
      
      case GECOResourceSetCreateFailureInvalidJobOwner: {
        fprintf(stderr, "ERROR: the user or group that owns job %ld.%ld does not exist on this host\n", jobId, taskId);
        rc = ENOENT;
        break;
      }
      
      case GECOResourceSetCreateFailureNoStaticProperties:
      case GECOResourceSetCreateFailureNoRequestedResources:
      case GECOResourceSetCreateFailureNoGrantedResources: {
        fprintf(stderr, "ERROR: resource information not available for job %ld.%ld; it either does not exist or is not running\n", jobId, taskId);
        rc = EINVAL;
        break;
      }
      
    }
  }
  
  if ( theResources ) {
    if ( serializeToPath ) {
      GECOResourceSetSerialize(theResources, serializeToPath);
    } else {
      if ( thisHostOnly ) {
        GECOResourceSetExportForNodeName(theResources, exportMode, thisHost);
      } else {
        GECOResourceSetExport(theResources, exportMode);
      }
    }
    GECOResourceSetDestroy(theResources);
  }
  
  // Shutdown libxml:
  xmlCleanupParser();
  
  return rc;
}
