/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  gecod.c
 *  
 *  Daemon that intercepts process launches in search of Grid Engine
 *  job pieces, and automatically creates cgroups to mirror requested
 *  resource limits.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECORunloop.h"
#include "GECOResource.h"
#include "GECOCGroup.h"
#include "GECOIntegerSet.h"
#include "GECOPidToJobIdMap.h"
#include "GECOJob.h"
#include "GECOLog.h"
#include "GECOQuarantine.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>

//

#ifndef GECO_GECOD_VERSION
#error GECO_GECOD_VERSION must be defined
#endif
const char *GECODVersionString = GECO_GECOD_VERSION;

#ifndef GECOD_STARTUP_RETRY_COUNT
#define GECOD_STARTUP_RETRY_COUNT   6
#endif

static unsigned int GECODDefaultStartupRetryCount = GECOD_STARTUP_RETRY_COUNT;

#ifndef GECOD_RECEIVE_TIMEOUT
#define GECOD_RECEIVE_TIMEOUT       5
#endif

static unsigned int GECODDefaultReceiveTimeout = GECOD_RECEIVE_TIMEOUT;

#ifndef GECOD_SEND_TIMEOUT
#define GECOD_SEND_TIMEOUT          5
#endif

static unsigned int GECODDefaultSendTimeout = GECOD_SEND_TIMEOUT;

#ifndef GECOD_QUARANTINE_SOCKET
#define GECOD_QUARANTINE_SOCKET     "path:/tmp/gecod_quarantine"
#endif

static const char *GECODDefaultQuarantineSocket = GECOD_QUARANTINE_SOCKET;

//

static GECORunloopRef GECODRunloop = NULL;

static GECOJobRef (*GECODJobCreationFunction)(long int, long int) = GECOJobCreateWithJobIdentifier;

static GECOPidToJobIdMapRef GECODPidMappings = NULL;

#include "GECODNetlinkSocket.c"

#include "GECODQuarantineSocket.c"

#include <getopt.h>

enum {
  GECODCliOptHelp             = 'h',
  GECODCliOptVerbose          = 'v',
  GECODCliOptQuiet            = 'q',
  GECODCliOptEnable           = 'e',
  GECODCliOptDisable          = 'd',
  GECODCliOptDaemonize        = 'D',
  GECODCliOptPidFile          = 'p',
  GECODCliOptLogFile          = 'l',
  GECODCliOptStartupRetry     = 'r',
  GECODCliOptStateDir         = 'S',
  GECODCliOptCGroupMountPoint = 'm',
  GECODCliOptCGroupSubGroup   = 's',
  GECODCliOptQuarantineSocket = 'Q',
  GECODCliOptReceiveTimeout   = 'R',
  GECODCliOptSendTimeout      = 't',
  GECODCliOptNoQstat          = 1001
};

const char *GECODCliOptString = "hvqe:d:Dp:l?r:S:m:s:Q:R:t:";

const struct option GECODCliOpts[] = {
                  { "help",                 no_argument,          NULL,         GECODCliOptHelp },
                  { "verbose",              no_argument,          NULL,         GECODCliOptVerbose },
                  { "quiet",                no_argument,          NULL,         GECODCliOptQuiet },
                  { "disable",              required_argument,    NULL,         GECODCliOptDisable },
                  { "enable",               required_argument,    NULL,         GECODCliOptEnable },
                  { "daemon",               no_argument,          NULL,         GECODCliOptDaemonize },
                  { "pidfile",              required_argument,    NULL,         GECODCliOptPidFile },
                  { "logfile",              optional_argument,    NULL,         GECODCliOptLogFile },
                  { "quarantine-socket",    required_argument,    NULL,         GECODCliOptQuarantineSocket },
                  { "startup-retry",        required_argument,    NULL,         GECODCliOptStartupRetry },
                  { "state-dir",            required_argument,    NULL,         GECODCliOptStateDir },
                  { "cgroup-mountpoint",    required_argument,    NULL,         GECODCliOptCGroupMountPoint },
                  { "cgroup-subgroup",      required_argument,    NULL,         GECODCliOptCGroupSubGroup },
                  { "receive-timeout",      required_argument,    NULL,         GECODCliOptReceiveTimeout },
                  { "send-timeout",         required_argument,    NULL,         GECODCliOptSendTimeout },
                  { "no-qstat",             no_argument,          NULL,         GECODCliOptNoQstat },
                  { NULL,                   0,                    0,             0  }
                };

//

void
usage(
  const char    *exe
)
{
  GECOCGroupSubsystem   subsysId;
  bool                  needComma = false;
  
  printf(
      "usage:\n\n"
      "  %s {options}\n\n"
      " options:\n\n"
      "  --help/-h                            show this information\n"
      "  --verbose/-v                         increase the verbosity level (may be used\n"
      "                                       multiple times)\n"
      "  --quiet/-q                           decrease the verbosity level (may be used\n"
      "                                       multiple times)\n"
      "  --enable/-e <subsystem>              enable checks against the given cgroup\n"
      "                                       subsystem\n"
      "  --disable/-d <subsystem>             disable checks against the given cgroup\n"
      "                                       subsystem\n"
      "  --daemon/-D                          run as a daemon\n"
      "  --pidfile/-p <path>                  file in which our pid should be written\n"
      "  --logfile/-l {<path>}                all logging should be written to <path>; if\n"
      "                                       <path> is omitted stderr is used\n"
      "  --quarantine-socket/-Q <bind-info>   if an absolute path is provided, opens a world-writable\n"
      "                                       named socket at the given path; if an integer is\n"
      "                                       provided, listens on localhost:<port#>\n"
      "                                       (default: %s)\n"
      "  --state-dir/-S <path>                directory to which gecod should write resource\n"
      "                                       cache files, traces, etc.  The <path> should\n"
      "                                       be on a network filesystem shared between all\n"
      "                                       nodes in the cluster\n"
      "                                       (default: %s)\n"
      "  --cgroup-mountpoint/-m <path>        directory in which cgroup subsystems are\n"
      "                                       mounted (default: %s)\n"
      "  --cgroup-subgroup/-s <name>          specify the path (relative to the cgroup subgroups'\n"
      "                                       mount points) in which GECO will create per-job\n"
      "                                       subgroups (default: %s)\n"
      "  --startup-retry/-r #                 if cgroup or socket setup fails, retry this many\n"
      "                                       times; specify -1 for unlimited retries\n"
      "                                       (default: %u %s)\n"
      "  --receive-timeout/-R #               when receiving messages on sockets only wait this many\n"
      "                                       seconds before considering the attempt timed-out\n"
      "                                       (default: %u %s)\n"
      "  --send-timeout/-t #                  when sending messages on sockets only wait this many\n"
      "                                       seconds before considering the attempt timed-out\n"
      "                                       (default: %u %s)\n"
      "  --no-qstat                           by default, job information is initially fetched from\n"
      "                                       the qmaster via qstat and then cached for the duration\n"
      "                                       of the job; set this flag if you pre-create the cached\n"
      "                                       copy inside the state directory\n"
      "\n"
      "  <bind-info> can be:\n"
      "    service:<named service>|#          open quarantine socket bound to localhost and the given\n"
      "                                       tcp service by name or port number\n"
      "    path:<path>                        open quarantine socket bound to the filesystem at the\n"
      "                                       given path\n"
      "\n"
      "  <subsystem> should be one of:\n\n"
      "    ",
      exe,
      GECODDefaultQuarantineSocket,
      GECOGetStateDir(),
      GECOCGroupGetPrefix(),
      GECOCGroupGetSubGroup(),
      GECODDefaultStartupRetryCount, (GECODDefaultStartupRetryCount == 1) ? "retry" : "retries",
      GECODDefaultReceiveTimeout, (GECODDefaultReceiveTimeout == 1) ? "second" : "seconds",
      GECODDefaultSendTimeout, (GECODDefaultSendTimeout == 1) ? "second" : "seconds"
    );
  
  subsysId = GECOCGroupSubsystem_min;
  while ( subsysId < GECOCGroupSubsystem_max ) {
    printf("%s%s", ( subsysId > GECOCGroupSubsystem_min ? ", " : "" ), GECOCGroupSubsystemToCString(subsysId));
    subsysId++;
  }
  printf(
      "\n"
      "\n"
      "  Subsystems enabled by default are:\n"
      "\n"
      "    "
    );
  subsysId = GECOCGroupSubsystem_min;
  while ( subsysId < GECOCGroupSubsystem_max ) {
    if ( GECOCGroupGetSubsystemIsManaged(subsysId) ) {
      printf("%s%s", ( needComma ? ", " : "" ), GECOCGroupSubsystemToCString(subsysId));
      needComma = true;
    }
    subsysId++;
  }
  printf(
      "\n"
      "\n"
      " %s - $Id$\n"
      "\n",
      GECODVersionString
    );
}

//

void
GECODHandleSignal(
  int     signo
)
{
  switch ( signo ) {
    
    case SIGALRM:
      break;
      
    case SIGTERM:
    case SIGINT: {
      GECORunloopSetShouldExitRunloop(GECODRunloop, true);
      break;
    }
  
  }
}

//

int
main(
  int                 argc,
  char                **argv
)
{
  const char          *exe = argv[0];
  int                 optch, optidx;
  
  int                 rc = 0;
  bool                isDaemon = false;
  char                *cgroupDir = NULL;
  char                *cgroupSubGroup = NULL;
  char                *stateDir = NULL;
  char                *pidFile = NULL;
  char                *logFile = NULL;
  char                *quarantineSocketStr = NULL;
  unsigned int        startupRetryCount = GECODDefaultStartupRetryCount;
  unsigned int        receiveTimeout = GECODDefaultReceiveTimeout;
  unsigned int        sendTimeout = GECODDefaultSendTimeout;
  bool                shouldDisableQstat = false;
  
  if ( getuid() != 0 ) {
		fprintf(stderr, "ERROR:  %s must be run as root\n", exe);
		return EPERM;
	}
  
  // Init libxml:
  xmlInitParser();
  LIBXML_TEST_VERSION
  
  // Check for arguments:
  while ( (optch = getopt_long(argc, argv, GECODCliOptString, GECODCliOpts, &optidx)) != -1 ) {
    GECOIntegerSetRef   *whichSet = NULL;
    
    switch ( optch ) {
      
      case GECODCliOptHelp: {
        usage(exe);
        exit(0);
      }
      
      case GECODCliOptVerbose: {
        GECOLogIncLevel(GECOLogGetDefault());
        break;
      }
      
      case GECODCliOptQuiet: {
        GECOLogDecLevel(GECOLogGetDefault());
        break;
      }
      
      case GECODCliOptEnable:
      case GECODCliOptDisable: {
        if ( optarg ) {
          GECOCGroupSubsystem   subsysId = GECOCGroupCStringToSubsystem(optarg);
          
          if ( subsysId != GECOCGroupSubsystem_invalid ) {
            GECOCGroupSetSubsystemIsManaged(subsysId, (optch == GECODCliOptEnable) ? true : false);
          } else {
            fprintf(stderr, "ERROR:  invalid cgroup subsystem specified: %s\n", optarg);
            exit(EINVAL);
          }
        }
        break;
      }
      
      case GECODCliOptDaemonize: {
        isDaemon = true;
        break;
      }
        
      case GECODCliOptPidFile: {
        if ( optarg && *optarg ) {
          pidFile = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --pidfile/-p option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptLogFile: {
        if ( optarg && *optarg ) logFile = optarg;
        break;
      }
      
      case GECODCliOptStateDir: {
        if ( optarg && *optarg ) {
          stateDir = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --state-dir/-S option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptCGroupMountPoint: {
        if ( optarg && *optarg ) {
          cgroupDir = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --cgroup-mountpoint/-c option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptCGroupSubGroup: {
        if ( optarg && *optarg ) {
          cgroupSubGroup = optarg;
        } else {
          fprintf(stderr, "ERROR:  no name provided with --cgroup-subgroup/-s option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptStartupRetry: {
        int     tmpInt;
        
        if ( optarg && *optarg && GECO_strtoi(optarg, &tmpInt, NULL) ) {
          startupRetryCount = (tmpInt < 0) ? -1 : tmpInt;
        } else {
          fprintf(stderr, "ERROR:  invalid value provided with --startup-retry/-S: %s\n", optarg);
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptQuarantineSocket: {
        if ( optarg && *optarg ) {
          quarantineSocketStr = optarg;
        } else {
          fprintf(stderr, "ERROR:  no port#/path provided with --quarantine-socket/-Q option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptReceiveTimeout: {
        long int    tmpInt;
        
        if ( optarg && *optarg && GECO_strtol(optarg, &tmpInt, NULL) && (tmpInt >= 0) && (tmpInt <= UINT_MAX) ) {
          receiveTimeout = tmpInt;
        } else {
          fprintf(stderr, "ERROR:  invalid value provided with --receive-timeout/-R: %s\n", optarg);
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptSendTimeout: {
        long int    tmpInt;
        
        if ( optarg && *optarg && GECO_strtol(optarg, &tmpInt, NULL) && (tmpInt >= 0) && (tmpInt <= UINT_MAX) ) {
          sendTimeout = tmpInt;
        } else {
          fprintf(stderr, "ERROR:  invalid value provided with --send-timeout/-t: %s\n", optarg);
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptNoQstat: {
        shouldDisableQstat = true;
        break;
      }

    }
  }
  
  if ( isDaemon ) {
    if ( daemon(0, 1) != 0 ) {
      fprintf(stderr, "ERROR:  unable to daemonize (errno = %d)", errno);
      exit(EINVAL);
    }
  }
  
  // Setup signal handling:
  signal(SIGHUP, SIG_IGN);
  signal(SIGALRM, GECODHandleSignal);
  signal(SIGTERM, GECODHandleSignal);
  signal(SIGINT, GECODHandleSignal);
  
  // Open the pid file if one was requested:
  if ( pidFile ) {
    FILE    *fptr = fopen(pidFile, "w");
    
    if ( fptr ) {
      fprintf(fptr, "%lld", (long long int)getpid());
      fclose(fptr);
    } else {
      fprintf(stderr, "ERROR:  unable to write to pidfile %s\n", pidFile);
    }
  }
  
  // Get the log file opened if an alternate path was requested:
  if ( logFile ) {
    if ( ! freopen(optarg, "a", stderr) ) {
      fprintf(stderr, "ERROR:  unable to open logfile %s\n", optarg);
      rc = errno;
      goto gecod_earlyExit;
    }
  }
  
  // Send stdin and stdout to /dev/null like any well-behaved daemon:
  if ( isDaemon ) {
    freopen("/dev/null", "r", stdin);
    freopen("/dev/null", "w", stdout);
  }
  
  // Get the GECO shared state directory ready to roll:
  if ( ! GECOSetStateDir(stateDir) ) {
    GECO_ERROR("unable to setup state directory %s (errno = %d)\n", ( stateDir ? stateDir : GECOGetStateDir() ), errno);
    rc = errno;
    goto gecod_earlyExit;
  }
  
  // Make sure the cgroup mountpoint container is ready:
  if ( ! GECOCGroupSetPrefix(cgroupDir) ) {
    GECO_ERROR("unable to setup cgroup prefix directory %s (errno = %d)\n", ( cgroupDir ? cgroupDir : GECOCGroupGetPrefix() ), errno);
    rc = errno;
    goto gecod_earlyExit;
  }
  
  // Validate the cgroup subgroup name if one was explicitly provided on the CLI:
  if ( cgroupSubGroup && ! GECOCGroupSetSubGroup(cgroupSubGroup) ) {
    GECO_ERROR("invalid cgroup subgroup %s (errno = %d)\n", cgroupSubGroup, errno);
    rc = errno;
    goto gecod_earlyExit;
  }
  
  // Qstat use denied?
  if ( shouldDisableQstat ) {
    GECO_WARN(" !! All resource information must be pre-populated for jobs since qstat use is disabled !!");
    GECO_WARN(" !! Per-job data should be serialized to %s/resources/<jobid>.<taskid> using geco-rsrcinfo !!", GECOGetStateDir());
    GECODJobCreationFunction = GECOJobCreateWithJobIdentifierFromResourceCache;
  }
  
  GECO_ERROR(" Grid Engine Cgroup Orchestrator - %s", GECODVersionString);
  GECO_ERROR(" Grid Engine Cgroup Orchestrator library - %s", GECOLibraryVersion);

retry_cgroup_init:

  // Try to get the GECO cgroup subgroups created:
  if ( ! GECOCGroupInitSubsystems() ) {
    if ( startupRetryCount != 0 ) {
      if ( startupRetryCount > 0 ) startupRetryCount--;
      GECO_WARN("failed to complete initial setup of GECO cgroup subgroups...retrying in 15 seconds");
      sleep(15);
      goto retry_cgroup_init;
    } else {
      GECO_ERROR("failed to complete initial setup of GECO cgroup subgroups.");
      rc = EPERM;
      goto gecod_earlyExit;
    }
  }
  
  GECOQuarantineSocket      quarantineSocket;
  GECODNetlinkSocket        nlSocket;
  bool                      ok;
  
  ok = GECOQuarantineSocketOpenServer(
          GECOQuarantineSocketTypeInferred,
          (quarantineSocketStr ? quarantineSocketStr : GECODDefaultQuarantineSocket),
          startupRetryCount,
          receiveTimeout,
          sendTimeout,
          &quarantineSocket
        );
  if ( ok ) {
    rc = GECODNetlinkSocketInit(&nlSocket);
    
    if ( rc == 0 ) {
      // Create the runloop:
      GECODRunloop = GECORunloopCreate();
      if ( GECODRunloop ) {
        GECO_DEBUG("created runloop");
        
        GECODPidMappings = GECOPidToJobIdMapCreate(0);
        if ( GECODPidMappings ) {
          GECO_DEBUG("created pid mapping table");
          
          // Initialize the job management component:
          GECOJobInit();
          GECO_DEBUG("initialized job management");
          
          // Add the quarantine socket to the runloop:
          GECORunloopAddPollingSource(GECODRunloop, &quarantineSocket, &GECODQuarantineSocketCallbacks, GECOPollingSourceFlagStaticFileDescriptor);
          GECO_DEBUG("quarantine socket polling source added to runloop");
          
          // Add the netlink socket to the runloop:
          GECORunloopAddPollingSource(GECODRunloop, &nlSocket, &GECODNetlinkSocketCallbacks, 0);
          GECO_DEBUG("netlink socket polling source added to runloop");
          
          // Run until something says we're done:
          GECO_DEBUG("entering runloop");
          rc = GECORunloopRun(GECODRunloop);
          
          // Deinitialize the job management component:
          GECOJobDeinit();
          GECO_DEBUG("shutting down job management");
        } else {
          GECO_ERROR("Unable to allocate a pid-to-job-id mapping table");
        }
        
        // All done with our runloop:
        GECO_DEBUG("destroying runloop");
        GECORunloopDestroy(GECODRunloop);
      } else {
        GECO_ERROR("Unable to allocate runloop");
      }
    } else {
      GECO_ERROR("Unable to create netlink socket (errno = %d)", rc);
    }
    
    // Get rid of the quarantine socket:
    GECOQuarantineSocketClose(&quarantineSocket);
  }
  
  // Shutdown all GECO cgroup subgroups:
  GECOCGroupShutdownSubsystems();

gecod_earlyExit:
  // Shutdown libxml:
  xmlCleanupParser();
  
  // Drop the pid file if we had one:
  if ( pidFile ) unlink(pidFile);
  
  return rc;
}
