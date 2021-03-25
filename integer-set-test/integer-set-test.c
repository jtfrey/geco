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

#include "GECOIntegerSet.h"

int
main(
  int         argc,
  char        **argv
)
{
  GECOIntegerSetRef     initSet = GECOIntegerSetCreate();
  int                   argn = 1;
  
  while ( argn < argc ) {
    long int            value;
    
    if ( GECO_strtol(argv[argn], &value, NULL) ) {
      GECOIntegerSetAddInteger(initSet, value);
    }
    argn++;
  }
  
  printf("Integer set:  ");
  GECOIntegerSetSummarizeToStream(initSet, stdout);
  printf("\n\n");
  
  GECOIntegerSetDebug(initSet, stdout); printf("\n\n");
  
  GECOIntegerSetRef     duplSet = GECOIntegerSetCreateConstantCopy(initSet);
  
  printf("const Integer set:  ");
  GECOIntegerSetSummarizeToStream(duplSet, stdout);
  printf("\n\n");
  
  GECOIntegerSetDebug(duplSet, stdout); printf("\n\n");
  
  printf("%d in set: %d\n", 1014, GECOIntegerSetContains(duplSet, 1014));
  
  GECOIntegerSetDestroy(duplSet);
  GECOIntegerSetDestroy(initSet);
  
  return 0;
}
