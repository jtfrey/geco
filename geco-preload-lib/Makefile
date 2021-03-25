#
#
#

-include ../Makefile.inc

#CPPFLAGS                        += -I../lib -I$(LIBCONFUSE_PREFIX)/include -DGECO_LDPRELOAD_VALUE='"$(LIBDIR)/$(TARGET).so"'
CPPFLAGS			+= -I../lib -I$(LIBCONFUSE_PREFIX)/include -DGECO_LDPRELOAD_VALUE='"/lib64/$(TARGET).so"'

install_LDFLAGS                 := $(LDFLAGS) -L$(LIBDIR) -Wl,--rpath,$(LIBDIR) -L$(LIBCONFUSE_PREFIX)/lib64 -Wl,--rpath,$(LIBCONFUSE_PREFIX)/lib64 -Wl,-init,GECOExecWrapperInit
LDFLAGS                         += -L../lib -Wl,--rpath,$(shell cd ../lib ; pwd) -L$(LIBCONFUSE_PREFIX)/lib64 -Wl,--rpath,$(LIBCONFUSE_PREFIX)/lib64 -Wl,-init,GECOExecWrapperInit

install_LIBS                    := $(LIBS) -lxml2 -lconfuse -Wl,-Bstatic -lGECO -Wl,-Bdynamic
LIBS                            += -lxml2 -lconfuse -Wl,-Bstatic -lGECO -Wl,-Bdynamic

#
##
#

TARGET				= libGECOLdPreload

OBJECTS				= GECOExecWrapper.o

HEADERS				= 

default: $(TARGET).so

install: install_$(TARGET).so

-include ../Makefile.rules

