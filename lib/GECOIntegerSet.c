/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOIntegerSet.c
 *
 *  Set of integer values.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOIntegerSet.h"

#include <limits.h>

typedef GECOIntegerSetRef (*GECOIntegerSetCopyCallback)(GECOIntegerSetRef setOfIntegers);
typedef void              (*GECOIntegerSetDeallocCallback)(GECOIntegerSetRef setOfIntegers);
typedef void              (*GECOIntegerSetPrintCallback)(GECOIntegerSetRef setOfIntegers, FILE *stream);
typedef void              (*GECOIntegerSetDebugCallback)(GECOIntegerSetRef setOfIntegers, FILE *stream);
typedef bool              (*GECOIntegerSetGetIntegerAtIndexCallback)(GECOIntegerSetRef setOfIntegers, unsigned int index, GECOInteger *anInteger);
typedef bool              (*GECOIntegerSetContainsCallback)(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);
typedef bool              (*GECOIntegerSetAddIntegerCallback)(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);
typedef bool              (*GECOIntegerSetRemoveIntegerCallback)(GECOIntegerSetRef setOfIntegers, GECOInteger anInteger);

typedef struct {
  const char                              *subType;
  GECOIntegerSetCopyCallback              copy;
  GECOIntegerSetDeallocCallback           dealloc;
  GECOIntegerSetPrintCallback             print;
  GECOIntegerSetDebugCallback             debug;
  GECOIntegerSetGetIntegerAtIndexCallback getIntegerAtIndex;
  GECOIntegerSetContainsCallback          contains;
  GECOIntegerSetAddIntegerCallback        addInteger;
  GECOIntegerSetRemoveIntegerCallback     removeInteger;
} GECOIntegerSetImpl;

//

typedef struct __GECOIntegerSet {
  GECOIntegerSetImpl    impl;
  unsigned int          count;
  bool                  isConstant, isStatic;
} GECOIntegerSet;

//

void
__GECOIntegerSetInit(
  GECOIntegerSet        *setOfIntegers,
  GECOIntegerSetImpl    *implementation,
  bool                  isConstant
)
{
  memcpy(&setOfIntegers->impl, implementation, sizeof(*implementation));
  setOfIntegers->count = 0;
  setOfIntegers->isConstant = isConstant;
  setOfIntegers->isStatic = false;
}

//
#pragma mark -
//

typedef struct {
  GECOIntegerSet        base;
  bool                  isFixedCapacity;
  unsigned int          capacity;
  GECOInteger           *array;
} GECOSimpleArrayIntegerSet;

//

GECOSimpleArrayIntegerSet* __GECOSimpleArrayIntegerSetAlloc(unsigned int capacity, bool isFixedCapacity);

//

GECOIntegerSetRef
GECOSimpleArrayIntegerSetCopy(
  GECOIntegerSetRef   setOfIntegers
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  GECOSimpleArrayIntegerSet   *newSet;
  
  newSet = __GECOSimpleArrayIntegerSetAlloc(theSet->capacity, theSet->isFixedCapacity);
  if ( newSet ) {
    memcpy(newSet, theSet, sizeof(GECOIntegerSet));
    if ( newSet->base.count ) memcpy(newSet->array, theSet->array, newSet->base.count * sizeof(GECOInteger));
  }
  return (GECOIntegerSetRef)newSet;
}

//

void
GECOSimpleArrayIntegerSetDealloc(
  GECOIntegerSetRef   setOfIntegers
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  
  if ( theSet->array && ! theSet->isFixedCapacity ) free((void*)theSet->array);
  free((void*)setOfIntegers);
}

//

void
GECOSimpleArrayIntegerSetPrint(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  unsigned int                iMin = 0, iMax = setOfIntegers->count;
  
  while ( iMin < iMax ) {
    fprintf(stream, "%s%ld", (iMin ? "," : ""), theSet->array[iMin]);
    iMin++;
  }
}

//

void
GECOSimpleArrayIntegerSetDebug(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;

  fprintf(stream, "isFixedCapacity: %s; capacity: %u; [ ", (theSet->isFixedCapacity ? "true" : "false"), theSet->capacity);
  GECOSimpleArrayIntegerSetPrint(setOfIntegers, stream);
  fprintf(stream, " ]");
}

//

bool
GECOSimpleArrayIntegerSetGetIntegerAtIndex(
  GECOIntegerSetRef   setOfIntegers,
  unsigned int        index,
  GECOInteger         *anInteger
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  
  if ( index < theSet->base.count ) {
    *anInteger = theSet->array[index];
    return true;
  }
  return false;
}

//

bool
GECOSimpleArrayIntegerSetContains(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  unsigned int            iMin = 0, iMax = theSet->base.count;
    
  while ( iMin < iMax ) {
    unsigned int      i = iMin + ((iMax - iMin) / 2);
    
    if ( theSet->array[i] == anInteger ) return true;
    
    if ( theSet->array[i] < anInteger ) {
      if ( i == UINT_MAX ) break;
      iMin = i + 1;
    } else {
      if ( i == 0 ) break;
      iMax = i - 1;
    }
  }
  return false;
}

//

bool
__GECOSimpleArrayIntegerSetIncreaseCapacity(
  GECOSimpleArrayIntegerSet     *setOfIntegers
)
{
  unsigned int      newCapacity;
  GECOInteger       *newPtr;
  
  if ( setOfIntegers->isFixedCapacity || setOfIntegers->base.isConstant ) return false;
  
  newCapacity = setOfIntegers->capacity + 32;
  newPtr = realloc(setOfIntegers->array, newCapacity * sizeof(GECOInteger));
  if ( newPtr ) {
    setOfIntegers->array = newPtr;
    setOfIntegers->capacity = newCapacity;
    return true;
  }
  return false;
}

//

bool
GECOSimpleArrayIntegerSetAddInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  unsigned int            i = 0;
  
  if ( theSet->base.isConstant ) return false;
  
  while ( i < theSet->base.count ) {
    if ( anInteger < theSet->array[i] ) break;
    if ( anInteger == theSet->array[i] ) return false;
    i++;
  }
  
  if ( theSet->base.count == theSet->capacity ) {
    if ( ! __GECOSimpleArrayIntegerSetIncreaseCapacity(theSet) ) return false;
  }
  
  // Shift any latter values up:
  if ( i < theSet->base.count ) memmove(&theSet->array[i + 1], &theSet->array[i], (theSet->base.count - i) * sizeof(GECOInteger));
  theSet->array[i] = anInteger;
  return true;
}

//

bool
GECOSimpleArrayIntegerSetRemoveInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  GECOSimpleArrayIntegerSet   *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  unsigned int            iMin = 0, iMax = theSet->base.count;
  
  if ( theSet->base.isConstant ) return false;
  
  while ( iMin < iMax ) {
    unsigned int      i = iMin + ((iMax - iMin) / 2);
    
    if ( theSet->array[i] == anInteger ) {
      if ( i < iMax - 1 ) memmove(&theSet->array[i], &theSet->array[i + 1], (iMax - i - 1) * sizeof(GECOInteger));
      return true;
    }
    
    if ( theSet->array[i] < anInteger ) {
      if ( i == UINT_MAX ) break;
      iMin = i + 1;
    } else {
      if ( i == 0 ) break;
      iMax = i - 1;
    }
  }
  return false;
}

//

static GECOIntegerSetImpl GECOSimpleArrayIntegerSetImpl = {
                              .subType = "GECOSimpleArrayIntegerSet",
                              .copy = GECOSimpleArrayIntegerSetCopy,
                              .dealloc = GECOSimpleArrayIntegerSetDealloc,
                              .print = GECOSimpleArrayIntegerSetPrint,
                              .debug = GECOSimpleArrayIntegerSetDebug,
                              .getIntegerAtIndex = GECOSimpleArrayIntegerSetGetIntegerAtIndex,
                              .contains = GECOSimpleArrayIntegerSetContains,
                              .addInteger = GECOSimpleArrayIntegerSetAddInteger,
                              .removeInteger = GECOSimpleArrayIntegerSetRemoveInteger
                            };

//

GECOSimpleArrayIntegerSet*
__GECOSimpleArrayIntegerSetEmpty()
{
  static GECOSimpleArrayIntegerSet    emptySet = {
              .base = {
                  .impl = {
                      .subType = "GECOSimpleArrayIntegerSet",
                      .copy = GECOSimpleArrayIntegerSetCopy,
                      .dealloc = GECOSimpleArrayIntegerSetDealloc,
                      .print = GECOSimpleArrayIntegerSetPrint,
                      .debug = GECOSimpleArrayIntegerSetDebug,
                      .getIntegerAtIndex = GECOSimpleArrayIntegerSetGetIntegerAtIndex,
                      .contains = GECOSimpleArrayIntegerSetContains,
                      .addInteger = GECOSimpleArrayIntegerSetAddInteger,
                      .removeInteger = GECOSimpleArrayIntegerSetRemoveInteger
                    },
                  .count = 0,
                  .isConstant = true,
                  .isStatic = true
                },
              .isFixedCapacity = true,
              .capacity = 0,
              .array = NULL
            };
  return (GECOSimpleArrayIntegerSet*)&emptySet;
}

//

GECOSimpleArrayIntegerSet*
__GECOSimpleArrayIntegerSetAlloc(
  unsigned int      capacity,
  bool              isFixedCapacity
)
{
  GECOSimpleArrayIntegerSet  *newSet = NULL;
  size_t                  byteSize = sizeof(GECOSimpleArrayIntegerSet);
  
  if ( isFixedCapacity ) {
    if ( capacity == 0 ) return __GECOSimpleArrayIntegerSetEmpty();
    byteSize += capacity * sizeof(GECOInteger);
  }
  newSet = malloc(byteSize);
  if ( newSet ) {
    __GECOIntegerSetInit((GECOIntegerSet*)newSet, &GECOSimpleArrayIntegerSetImpl, false);
    if ( isFixedCapacity ) {
      newSet->array = ((void*)newSet) + sizeof(GECOSimpleArrayIntegerSet);
    } else if ( capacity > 0 ) {
      newSet->array = malloc(capacity * sizeof(GECOInteger));
      if ( ! newSet->array ) {
        free(newSet);
        return NULL;
      }
    } else {
      newSet->array = NULL;
    }
    newSet->capacity = capacity;
    newSet->isFixedCapacity = isFixedCapacity;
  }
  return newSet;
}

//
#if 0
#pragma mark -
#endif
//

enum {
  GECOIntegerSetElementSubTypeSingle = 1,
  GECOIntegerSetElementSubTypeRange = 2
};

typedef struct {
  int             subType;
} GECOIntegerSetElement;

typedef struct {
  int             subType;
  GECOInteger     value;
} GECOIntegerSetElementSingle;

typedef struct {
  int             subType;
  GECOInteger     low, high;
} GECOIntegerSetElementRange;

typedef struct {
  GECOIntegerSet        base;
  unsigned int          elementCount, singlesCount, rangesCount;
  GECOIntegerSetElement *elements;
} GECOMixedElementIntegerSet;

static size_t GECOMixedElementIntegerSetSizes[] = { 0, sizeof(GECOIntegerSetElementSingle), sizeof(GECOIntegerSetElementRange) };

#define GECOMixedElementIntegerSetIteratorBegin(S) \
    { \
      void *elements = (void*)((S)->elements); unsigned int eIdx = 0; \
      while ( eIdx < (S)->elementCount ) { \
        GECOIntegerSetElement *element = (GECOIntegerSetElement*)elements; \
        elements += GECOMixedElementIntegerSetSizes[element->subType];

#define GECOMixedElementIntegerSetIteratorEnd(S) \
        eIdx++; \
      } \
    }
//

GECOMixedElementIntegerSet* __GECOMixedElementIntegerSetAllocBare(unsigned int countOfSingles, unsigned int countOfRanges);

//

GECOIntegerSetRef
GECOMixedElementIntegerSetCopy(
  GECOIntegerSetRef   setOfIntegers
)
{
  GECOMixedElementIntegerSet  *theSet = (GECOMixedElementIntegerSet*)setOfIntegers;
  GECOMixedElementIntegerSet  *newSet = __GECOMixedElementIntegerSetAllocBare(theSet->singlesCount, theSet->rangesCount);
  
  if ( newSet && newSet->elements ) memcpy(newSet->elements, theSet->elements, newSet->singlesCount * sizeof(GECOIntegerSetElementSingle) + newSet->rangesCount * sizeof(GECOIntegerSetElementRange));
  return (GECOIntegerSetRef)newSet;
}

//

void
GECOMixedElementIntegerSetDealloc(
  GECOIntegerSetRef   setOfIntegers
)
{
  free((void*)setOfIntegers);
}

//

void
GECOMixedElementIntegerSetPrint(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  GECOMixedElementIntegerSet  *theSet = (GECOMixedElementIntegerSet*)setOfIntegers;
  unsigned int                index = 0;
  
  GECOMixedElementIntegerSetIteratorBegin(theSet);
  
    switch ( element->subType ) {
    
      case GECOIntegerSetElementSubTypeSingle: {
        GECOIntegerSetElementSingle *eSingle = (GECOIntegerSetElementSingle*)element;
        
        fprintf(stream, "%s%ld", (index ? "," : ""), eSingle->value);
        index++;
        break;
      }
      
      case GECOIntegerSetElementSubTypeRange: {
        GECOIntegerSetElementRange  *eRange = (GECOIntegerSetElementRange*)element;
        GECOInteger                 v = eRange->low;
        
        while ( v <= eRange->high ) {
          fprintf(stream, "%s%ld", (index ? "," : ""), v);
          if ( v == INT_MAX ) return;
          v++;
          index++;
        }
        break;
      }
      
    }
  
  GECOMixedElementIntegerSetIteratorEnd(theSet);
}

//

void
GECOMixedElementIntegerSetDebug(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  GECOMixedElementIntegerSet  *theSet = (GECOMixedElementIntegerSet*)setOfIntegers;
  unsigned int                index = 0;
  
  fprintf(stream, "elementCount: %u; singlesCount: %u; rangesCount: %u; { ", theSet->elementCount, theSet->singlesCount, theSet->rangesCount);
  GECOMixedElementIntegerSetIteratorBegin(theSet);
  
    switch ( element->subType ) {
    
      case GECOIntegerSetElementSubTypeSingle: {
        GECOIntegerSetElementSingle *eSingle = (GECOIntegerSetElementSingle*)element;
        
        fprintf(stream, "%s%u: %ld", (index ? ", " : ""), index, eSingle->value);
        index++;
        break;
      }
      
      case GECOIntegerSetElementSubTypeRange: {
        GECOIntegerSetElementRange  *eRange = (GECOIntegerSetElementRange*)element;
        
        fprintf(stream, "%s%u: [%ld, %ld]", (index ? ", " : ""), index, eRange->low, eRange->high);
        index++;
        break;
      }
    
    }
    
  GECOMixedElementIntegerSetIteratorEnd(theSet);
  fprintf(stream, " }");
}

//

bool
GECOMixedElementIntegerSetGetIntegerAtIndex(
  GECOIntegerSetRef   setOfIntegers,
  unsigned int        index,
  GECOInteger         *anInteger
)
{
  GECOMixedElementIntegerSet  *theSet = (GECOMixedElementIntegerSet*)setOfIntegers;
  
  GECOMixedElementIntegerSetIteratorBegin(theSet);
  
    switch ( element->subType ) {
    
      case GECOIntegerSetElementSubTypeSingle: {
        GECOIntegerSetElementSingle *eSingle = (GECOIntegerSetElementSingle*)element;
        
        if ( index == 0 ) {
          *anInteger = eSingle->value;
          return true;
        }
        index--;
        break;
      }
      
      case GECOIntegerSetElementSubTypeRange: {
        GECOIntegerSetElementRange  *eRange = (GECOIntegerSetElementRange*)element;
        GECOInteger                 v = eRange->low;
        
        while ( index && (v <= eRange->high) ) {
          index--;
          if ( v == INT_MAX ) return false;
          v++;
        }
        if ( index == 0 ) {
          *anInteger = v;
          return true;
        }
        break;
      }
    
    }
  
  GECOMixedElementIntegerSetIteratorEnd(theSet);
  
  return false;
}

//

bool
GECOMixedElementIntegerSetContains(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  GECOMixedElementIntegerSet  *theSet = (GECOMixedElementIntegerSet*)setOfIntegers;
  
  GECOMixedElementIntegerSetIteratorBegin(theSet);
  
    switch ( element->subType ) {
    
      case GECOIntegerSetElementSubTypeSingle: {
        GECOIntegerSetElementSingle *eSingle = (GECOIntegerSetElementSingle*)element;
        
        if ( anInteger == eSingle->value ) return true;
        break;
      }
      
      case GECOIntegerSetElementSubTypeRange: {
        GECOIntegerSetElementRange  *eRange = (GECOIntegerSetElementRange*)element;
        
        if ( anInteger >= eRange->low && anInteger <= eRange->high ) return true;
        break;
      }
    
    }
  
  GECOMixedElementIntegerSetIteratorEnd(theSet);
  
  return false;
}

//

bool
GECOMixedElementIntegerSetAddInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  return false;
}

//

bool
GECOMixedElementIntegerSetRemoveInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  return false;
}

//

static GECOIntegerSetImpl GECOMixedElementIntegerSetImpl = {
                              .subType = "GECOMixedElementIntegerSet",
                              .dealloc = GECOMixedElementIntegerSetDealloc,
                              .print = GECOMixedElementIntegerSetPrint,
                              .debug = GECOMixedElementIntegerSetDebug,
                              .getIntegerAtIndex = GECOMixedElementIntegerSetGetIntegerAtIndex,
                              .contains = GECOMixedElementIntegerSetContains,
                              .addInteger = GECOMixedElementIntegerSetAddInteger,
                              .removeInteger = GECOMixedElementIntegerSetRemoveInteger
                            };

//

size_t
__GECOMixedElementIntegerSetAnalyzeArray(
  unsigned int      count,
  GECOInteger       *set,
  unsigned int      *countOfSingles,
  unsigned int      *countOfRanges
)
{
  size_t            totalBytes = 0;
  
  *countOfRanges = *countOfSingles = 0;
  if ( count ) {
    GECOInteger     low = *set, last = low, high = 0;
    bool            highIsSet = false;
    
    set++; count--;
    while ( count ) {
      if ( *set == last + 1 ) {
        high = *set;
        highIsSet = true;
      } else {
        // Create the next element:
        if ( highIsSet ) {
          totalBytes += sizeof(GECOIntegerSetElementRange);
          *countOfRanges += 1;
          highIsSet = false;
        } else {
          *countOfSingles += 1;
          totalBytes += sizeof(GECOIntegerSetElementSingle);
        }
        low = *set;
      }
      last = *set++;
      count--;
    }
    // Create the final element:
    if ( highIsSet ) {
      totalBytes += sizeof(GECOIntegerSetElementRange);
      *countOfRanges += 1;
    } else {
      totalBytes += sizeof(GECOIntegerSetElementSingle);
      *countOfSingles += 1;
    }
  }
  return totalBytes;
}

//

GECOMixedElementIntegerSet*
__GECOMixedElementIntegerSetAllocBare(
  unsigned int      countOfSingles,
  unsigned int      countOfRanges
)
{
  GECOMixedElementIntegerSet  *newSet = NULL;
  size_t                      byteSize = sizeof(GECOMixedElementIntegerSet) + countOfSingles * sizeof(GECOIntegerSetElementSingle) + countOfRanges * sizeof(GECOIntegerSetElementRange);
  
  newSet = malloc(byteSize);
  if ( newSet ) {
    __GECOIntegerSetInit((GECOIntegerSet*)newSet, &GECOMixedElementIntegerSetImpl, true);
    if ( (newSet->elementCount = countOfSingles + countOfRanges) > 0 ) {
      newSet->elements = ((void*)newSet) + sizeof(GECOMixedElementIntegerSet);
    } else {
      newSet->elements = NULL;
    }
    newSet->singlesCount = countOfSingles;
    newSet->rangesCount = countOfRanges;
  }
  return newSet;
}

//

GECOMixedElementIntegerSet*
__GECOMixedElementIntegerSetAlloc(
  unsigned int      count,
  GECOInteger       *set,
  unsigned int      countOfSingles,
  unsigned int      countOfRanges
)
{
  GECOMixedElementIntegerSet  *newSet = __GECOMixedElementIntegerSetAllocBare(countOfSingles, countOfRanges);
  
  if ( newSet ) {
    // Initialize the element records:
    void*           p = (void*)newSet->elements;
    GECOInteger     low = *set, last = low, high = 0;
    bool            highIsSet = false;
    
    set++; count--;
    while ( count ) {
      if ( *set == last + 1 ) {
        high = *set;
        highIsSet = true;
      } else {
        // Create the next element:
        if ( highIsSet ) {
          GECOIntegerSetElementRange    *R = (GECOIntegerSetElementRange*)p;
          
          R->subType = GECOIntegerSetElementSubTypeRange;
          R->low = low;
          R->high = high;
          p += sizeof(GECOIntegerSetElementRange);
          highIsSet = false;
        } else {
          GECOIntegerSetElementSingle   *S = (GECOIntegerSetElementSingle*)p;
          
          S->subType = GECOIntegerSetElementSubTypeSingle;
          S->value = low;
          p += sizeof(GECOIntegerSetElementSingle);
        }
        low = *set;
      }
      last = *set++;
      count--;
    }
    // Create the final element:
    if ( highIsSet ) {
      GECOIntegerSetElementRange    *R = (GECOIntegerSetElementRange*)p;
      
      R->subType = GECOIntegerSetElementSubTypeRange;
      R->low = low;
      R->high = high;
    } else {
      GECOIntegerSetElementSingle   *S = (GECOIntegerSetElementSingle*)p;
      
      S->subType = GECOIntegerSetElementSubTypeSingle;
      S->value = low;
    }
  }
  return newSet;
}

//
#if 0
#pragma mark -
#endif
//

GECOIntegerSetRef
GECOIntegerSetCreate(void)
{
  return (GECOIntegerSetRef)__GECOSimpleArrayIntegerSetAlloc(0, false);
}

//

GECOIntegerSetRef
GECOIntegerSetCreateWithCapacity(
  unsigned int    capacity
)
{
  return (GECOIntegerSetRef)__GECOSimpleArrayIntegerSetAlloc(capacity, true);
}

//

GECOIntegerSetRef
GECOIntegerSetCopy(
  GECOIntegerSetRef   setOfIntegers
)
{
  return setOfIntegers->impl.copy(setOfIntegers);
}

//

GECOIntegerSetRef
GECOIntegerSetCreateConstantCopy(
  GECOIntegerSetRef   setOfIntegers
)
{
  if ( setOfIntegers->isConstant ) return GECOIntegerSetCopy(setOfIntegers);
  
  // It's not constant implies it's a GECOSimpleArrayIntegerSet:
  GECOSimpleArrayIntegerSet     *theSet = (GECOSimpleArrayIntegerSet*)setOfIntegers;
  size_t                        asArray = theSet->base.count * sizeof(GECOInteger);
  unsigned int                  countOfRanges, countOfSingles;
  size_t                        asElements = __GECOMixedElementIntegerSetAnalyzeArray(theSet->base.count, theSet->array, &countOfSingles, &countOfRanges);
    
  if ( (asElements < asArray) && ((double)asElements / (double)asArray < 0.8) ) {
    return (GECOIntegerSetRef)__GECOMixedElementIntegerSetAlloc(theSet->base.count, theSet->array, countOfSingles, countOfRanges);
  }
  
  // Cheaper as a GECOSimpleArrayIntegerSet:
  GECOSimpleArrayIntegerSet     *otherSet = __GECOSimpleArrayIntegerSetAlloc(theSet->base.count, true);
  
  if ( otherSet && ! otherSet->base.isStatic ) {
    if ( theSet->base.count ) {
      memcpy(otherSet->array, theSet->array, theSet->base.count * sizeof(GECOInteger));
      otherSet->base.count = theSet->base.count;
    }
    otherSet->base.isConstant = true;
  }
  return (GECOIntegerSetRef)otherSet;
}

//

void
GECOIntegerSetDestroy(
  GECOIntegerSetRef   setOfIntegers
)
{
  if ( ! setOfIntegers->isConstant ) setOfIntegers->impl.dealloc(setOfIntegers);
}

//

unsigned int
GECOIntegerSetGetCount(
  GECOIntegerSetRef   setOfIntegers
)
{
  return setOfIntegers->count;
}

//

GECOInteger
GECOIntegerSetGetIntegerAtIndex(
  GECOIntegerSetRef   setOfIntegers,
  unsigned int        index
)
{
  GECOInteger         theInteger = 0;
  
  setOfIntegers->impl.getIntegerAtIndex(setOfIntegers, index, &theInteger);
  return theInteger;
}

//

bool
GECOIntegerSetContains(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  return setOfIntegers->impl.contains(setOfIntegers, anInteger);
}

//

void
GECOIntegerSetAddInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  if ( setOfIntegers->impl.addInteger(setOfIntegers, anInteger) ) setOfIntegers->count++;
}

//

void
GECOIntegerSetAddIntegerArray(
  GECOIntegerSetRef   setOfIntegers,
  unsigned int        count,
  GECOInteger         *theIntegers
)
{
  if ( setOfIntegers->isConstant ) return;
  while ( count-- ) GECOIntegerSetAddInteger(setOfIntegers, theIntegers[count]);
}

//

void
GECOIntegerSetAddIntegerRange(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         lowInteger,
  GECOInteger         highInteger
)
{
  if ( setOfIntegers->isConstant ) return;
  while ( lowInteger <= highInteger ) GECOIntegerSetAddInteger(setOfIntegers, lowInteger++);
}

//

void
GECOIntegerSetRemoveInteger(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         anInteger
)
{
  if ( setOfIntegers->impl.removeInteger(setOfIntegers, anInteger) ) setOfIntegers->count--;
}

//

void
GECOIntegerSetRemoveIntegerArray(
  GECOIntegerSetRef   setOfIntegers,
  unsigned int        count,
  GECOInteger         *theIntegers
)
{
  if ( setOfIntegers->isConstant ) return;
  while ( count-- ) GECOIntegerSetRemoveInteger(setOfIntegers, theIntegers[count]);
}

//

void
GECOIntegerSetRemoveIntegerRange(
  GECOIntegerSetRef   setOfIntegers,
  GECOInteger         lowInteger,
  GECOInteger         highInteger
)
{
  if ( setOfIntegers->isConstant ) return;
  while ( lowInteger <= highInteger ) GECOIntegerSetRemoveInteger(setOfIntegers, lowInteger++);
}

//

void
GECOIntegerSetSummarizeToStream(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  setOfIntegers->impl.print(setOfIntegers, stream);
}

//

void
GECOIntegerSetDebug(
  GECOIntegerSetRef   setOfIntegers,
  FILE                *stream
)
{
  fprintf(stream, "%s@%p { isConstant: %s; isStatic: %s; count: %u; ", setOfIntegers->impl.subType, setOfIntegers, (setOfIntegers->isConstant ? "true" : "false"), (setOfIntegers->isStatic ? "true" : "false"), setOfIntegers->count);
  setOfIntegers->impl.debug(setOfIntegers, stream);
  fprintf(stream, " }");
}
