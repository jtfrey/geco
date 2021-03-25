
PREFIX				= /opt/shared/univa/GECO
BINDIR				= $(PREFIX)/bin
ETCDIR				= $(PREFIX)/etc
SBINDIR				= $(PREFIX)/sbin
LIBDIR				= $(PREFIX)/lib64
INCDIR				= $(PREFIX)/include

GECOCGROUP_PREFIX		= /cgroup

GECO_STATEDIR			= /opt/geco

LIBXML2_PREFIX			= /usr

LIBCONFUSE_PREFIX		= /opt/shared/confuse/2

HWLOC_PREFIX			= /opt/shared/hwloc/1

CPPFLAGS			+= -I$(LIBXML2_PREFIX)/include/libxml2 -I$(HWLOC_PREFIX)/include

CFLAGS				+= -O0 -g -std=gnu99 -fPIC
#CFLAGS				+= -O3 -std=gnu99 -fPIC

LDFLAGS				+= -L$(LIBXML2_PREFIX)/lib64 -L$(HWLOC_PREFIX)/lib -Wl,--rpath,$(HWLOC_PREFIX)/lib

LIBS				+= -lxml2 -lhwloc

#
##
#

M4_DEFINES		= --define='__CGROUP_PREFIX__'=/cgroup \
                          --define='__GECO_STATEDIR__'="$(GECO_STATEDIR)" \
                          --define='__GECO_ETCDIR__'="$(ETCDIR)" \
                          --define='__GECO_BINDIR__'="$(BINDIR)" \
                          --define='__GECO_SBINDIR__'="$(SBINDIR)"

