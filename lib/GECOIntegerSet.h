/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOIntegerSet.h
 *
 *  Set of integer values.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOINTEGERSET_H__
#define __GECOINTEGERSET_H__

#include "GECO.h"

/*!
  @typedef GECOInteger
  @discussion
    The type of the integer values used by this API.
*/
typedef long int GECOInteger;

/*!
  @typedef GECOIntegerSetRef
  @discussion
    Type of a reference to a set of integers.
*/
typedef struct __GECOIntegerSet * GECOIntegerSetRef;

/*!
  @function GECOIntegerSetCreate
  @discussion
    Create a new (intially empty) integer set with no capacity limits.
  @result
    Returns NULL if a new set could not be allocated.
*/
GECOIntegerSetRef GECOIntegerSetCreate(void);

/*!
  @function GECOIntegerSetCreateWithCapacity
  @discussion
    Create a new (intially empty) integer set which can contain AT MOST
    capacity integer values.
  @result
    Returns NULL if a new set could not be allocated.
*/
GECOIntegerSetRef GECOIntegerSetCreateWithCapacity(unsigned int capacity);

/*!
  @function GECOIntegerSetCopy
  @discussion
    Create an integer set that is an exact duplicate of setOfIntegers.
  @result
    Returns NULL if a new set could not be allocated.
*/
GECOIntegerSetRef GECOIntegerSetCopy(GECOIntegerSetRef setOfIntegers);

/*!
  @function GECOIntegerSetCreateConstantCopy
  @discussion
    Create an integer set that contains the integer values present in
    setOfIntegers.  The resulting set cannot have values added/removed
    from it.
  @result
    Returns NULL if a new set could not be allocated.
*/
GECOIntegerSetRef GECOIntegerSetCreateConstantCopy(GECOIntegerSetRef setOfIntegers);

/*!
  @function GECOIntegerSetDestroy
  @discussion
    Invalidate setOfIntegers and deallocate any resources used by it.
  @result
    After calling this function, setOfIntegers is no longer a valid object.
*/
void GECOIntegerSetDestroy(GECOIntegerSetRef setOfIntegers);

/*!
  @function GECOIntegerSetGetCount
  @result
    Returns the number of integer values present in setOfIntegers.
*/
unsigned int GECOIntegerSetGetCount(GECOIntegerSetRef setOfIntegers);

/*!
  @function GECOIntegerSetGetIntegerAtIndex
  @result
    Returns the integer value present at index within setOfIntegers.  Zero (0) is
    returned if the index is out-of-range.
*/
GECOInteger GECOIntegerSetGetIntegerAtIndex(GECOIntegerSetRef setOfIntegers, unsigned int index);

/*!
  @function GECOIntegerSetContains
  @result
    Returns boolean true if anInteger is present within setOfIntegers.
*/
bool GECOIntegerSetContains(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);

/*!
  @function GECOIntegerSetAddInteger
  @discussion
    Attempts to add anInteger to setOfIntegers.
*/
void GECOIntegerSetAddInteger(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);

/*!
  @function GECOIntegerSetAddIntegerArray
  @discussion
    Attempts to add multiple integer values from array theIntegers (containing count
    elements) to setOfIntegers.
*/
void GECOIntegerSetAddIntegerArray(GECOIntegerSetRef setOfIntegers, unsigned int count, GECOInteger *theIntegers);

/*!
  @function GECOIntegerSetAddIntegerRange
  @discussion
    Attempts to add multiple integer values in the inclusive range [lowInteger, highInteger]
    to setOfIntegers.
*/
void GECOIntegerSetAddIntegerRange(GECOIntegerSetRef setOfIntegers, GECOInteger lowInteger, GECOInteger highInteger);

/*!
  @function GECOIntegerSetRemoveInteger
  @discussion
    Attempts to remove anInteger to setOfIntegers.
*/
void GECOIntegerSetRemoveInteger(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);

/*!
  @function GECOIntegerSetRemoveIntegerArray
  @discussion
    Attempts to remove from setOfIntegers multiple integer values from the array theIntegers
    (containing count elements).
*/
void GECOIntegerSetRemoveIntegerArray(GECOIntegerSetRef setOfIntegers, unsigned int count, GECOInteger *theIntegers);

/*!
  @function GECOIntegerSetRemoveIntegerRange
  @discussion
    Attempts to remove multiple integer values in the inclusive range [lowInteger, highInteger]
    from setOfIntegers.
*/
void GECOIntegerSetRemoveIntegerRange(GECOIntegerSetRef setOfIntegers, GECOInteger lowInteger, GECOInteger highInteger);

/*!
  @function GECOIntegerSetSummarizeToStream
  @discussion
    Write the integer values contained in setOfIntegers to the given stream as a
    comma-delimited textual list.
*/
void GECOIntegerSetSummarizeToStream(GECOIntegerSetRef setOfIntegers, FILE *stream);

/*!
  @function GECOIntegerSetDebug
  @discussion
    Write to stream a textual representation of the internal object structure of
    setOfIntegers.
*/
void GECOIntegerSetDebug(GECOIntegerSetRef setOfIntegers, FILE *stream);

#endif /* __GECOINTEGERSET_H__ */
