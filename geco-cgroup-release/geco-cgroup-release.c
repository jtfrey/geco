/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  geco-cgroup-release.c
 *  
 *  Standalone program that removes abandoned cgroup subgroups
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOCGroup.h"
#include "GECOLog.h"
#include <getopt.h>

const struct option geco_cli_options[] = {
                  { "help",                 no_argument,          NULL,         'h' },
                  { "verbose",              no_argument,          NULL,         'v' },
                  { "quiet",                no_argument,          NULL,         'q' },
                  { "subsystem",            required_argument,    NULL,         's' },
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
      "  %s {options} [path]\n\n"
      " options:\n\n"
      "  -h/--help                    show this information\n"
      "  -v/--verbose                 increase the verbosity level (may be used\n"
      "                                 multiple times)\n"
      "  -q/--quiet                   decrease the verbosity level (may be used\n"
      "                                 multiple times)\n"
      "  -s/--subsystem [name]        work with the named cgroup subsystem\n"
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
  const char                  *subsystem = NULL;
  
  // Check for arguments:
  while ( (optch = getopt_long(argc, argv, "hvqs:", geco_cli_options, NULL)) != -1 ) {
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
      
      case 's':
        if ( optarg && *optarg ) {
          subsystem = optarg;
        } else {
          fprintf(stderr, "ERROR:  no subsystem name provided with -s/--subsystem option\n");
          return EINVAL;
        }
        break;
    
    }
  }
  if ( optind < argc ) {
    int         argn = optind;
    
    // If we didn't get an explicit subsystem, use the command name:
    if ( ! subsystem ) {
      subsystem = exe + strlen(exe);
      while ( subsystem-- > exe ) {
        if ( *subsystem == '/' ) {
          subsystem++;
          break;
        }
      }
      GECO_DEBUG("inferred subsystem name %s from command %s", subsystem, exe);
    }
    while ( argn < argc ) {
      char        *subgroup = GECO_apathcatm(GECOCGroupGetPrefix(), subsystem, argv[argn], NULL);
      
      if ( subgroup ) {
        if ( GECOIsDirectory(subgroup) ) {
          if ( rmdir(subgroup) == 0 ) {
            GECO_DEBUG("removed subgroup %s from subsystem %s (%s)", argv[argn], subsystem, subgroup);
          } else {
            GECO_ERROR("failed to remove subgroup %s from subsystem %s (%s) (errno = %d)", argv[argn], subsystem, subgroup, errno);
            rc = errno;
          }
        } else {
          GECO_INFO("subgroup %s of subsystem %s (%s) does not exist", argv[argn], subsystem, subgroup);
        }
        free((void*)subgroup);
      } else {
        GECO_ERROR("failure in GECO_apathcatm()");
      }
      argn++;
    }
  } else {
    fprintf(stderr, "ERROR:  no subgroup path(s) provided\n");
    rc = EINVAL;
  }
  
  return rc;
}
