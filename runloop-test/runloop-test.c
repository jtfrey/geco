/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  runloop-test.c
 *  
 *  Standalone program that tests the GECORunloop functionality.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECORunloop.h"
#include "GECOLog.h"
#include <getopt.h>

const struct option geco_cli_options[] = {
                  { "help",                 no_argument,          NULL,         'h' },
                  { "verbose",              no_argument,          NULL,         'v' },
                  { "quiet",                no_argument,          NULL,         'q' },
                  { NULL,                   0,                    0,             0  }
                };

//

typedef struct {
  int         fd;
  const char  *path;
} runlooptest_pollingSource;

//

void
runlooptest_destroySource(
  GECOPollingSource   theSource
)
{
  runlooptest_pollingSource   *src = (runlooptest_pollingSource*)theSource;
  
  if ( src->fd >= 0 ) {
    printf("...closing fd %d\n", src->fd);
    close(src->fd);
  }
  printf("...deallocating %p (%s)\n", src, src->path);
  free((void*)src);
}

//

int
runlooptest_fileDescriptorForPolling(
  GECOPollingSource   theSource
)
{
  runlooptest_pollingSource   *src = (runlooptest_pollingSource*)theSource;
  
  printf("...runloop requested fd for %p (%s)\n", src, src->path);
  return src->fd;
}

//

void
runlooptest_didReceiveDataAvailable(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  runlooptest_pollingSource   *src = (runlooptest_pollingSource*)theSource;
  char                        c;
  
  printf("...data on %d (%s): ", src->fd, src->path);
  while ( read(src->fd, &c, 1) == 1 ) fputc(c, stdout);
  fputc('\n', stdout);
}

//

void
runlooptest_didReceiveClose(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  runlooptest_pollingSource   *src = (runlooptest_pollingSource*)theSource;
  
  close(src->fd);
  
  printf("...reopening %s\n", src->path);
  src->fd = open(src->path, O_RDONLY | O_NONBLOCK);
}

//

GECOPollingSourceCallbacks    ourCallbacks = {
                                            .destroySource = runlooptest_destroySource,
                                            .fileDescriptorForPolling = runlooptest_fileDescriptorForPolling,
                                            .shouldSourceClose = NULL,
                                            .willRemoveAsSource = NULL,
                                            .didAddAsSource = NULL,
                                            .didBeginPolling = NULL,
                                            .didReceiveDataAvailable = runlooptest_didReceiveDataAvailable,
                                            .didEndPolling = NULL,
                                            .didReceiveClose = runlooptest_didReceiveClose,
                                            .didRemoveAsSource = NULL
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
  int                         optch, argn;
  
  GECORunloopRef              ourRunloop = GECORunloopCreate();
  time_t                      expire = time(NULL) + 30;
  int                         rc = 0;
  
  // Check for arguments:
  while ( (optch = getopt_long(argc, argv, "hvq", geco_cli_options, NULL)) != -1 ) {
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
    
    }
  }
  
  argn = optind;
  while ( argn < argc ) {
    runlooptest_pollingSource   *src = malloc(sizeof(runlooptest_pollingSource));
    
    if ( src ) {
      src->path = argv[argn];
      src->fd = open(argv[argn], O_RDONLY | O_NONBLOCK);
      if ( src->fd >= 0 ) {
        if ( ! GECORunloopAddPollingSource(ourRunloop, src, &ourCallbacks, 0) ) {
          close(src->fd);
          free(src);
          printf("failed to add to runloop\n");
        }
      } else {
        free(src);
        printf("failed to open %s\n", argv[argn]);
      }
    }
    argn++;
  }
  
  GECORunloopRunUntil(ourRunloop, expire);
  
  GECORunloopDestroy(ourRunloop);
  
  return rc;
}
