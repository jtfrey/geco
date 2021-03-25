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

#include "GECO.h"

int
main(
  int         argc,
  char        **argv
)
{
  GECOPidTree   *theTree = GECOPidTreeCreate(true);
  
  if ( theTree ) {
    GECOPidTree *searchFrom = theTree;
    
    if ( argc > 1 ) {
      long int  thePid;
      
      if ( GECO_strtol(argv[1], &thePid, NULL) ) {
        searchFrom = GECOPidTreeGetNodeWithPid(theTree, (pid_t)thePid);
      }
    }
    if ( searchFrom ) GECOPidTreePrint(searchFrom, true, (searchFrom == theTree) ? true : false);
    GECOPidTreeDestroy(theTree);
  }
  return 0;
}
