#
#
#

-include ../Makefile.inc

TARGETS		= geco-preload-lib.conf \
                  geco-defaults.sh \
                  sysconfig-gecod

M4_DEFINES	= --define='__CGROUP_PREFIX__'=/cgroup \
                  --define='__GECO_STATEDIR__'="$(GECO_STATEDIR)" \
                  --define='__GECO_ETCDIR__'="$(ETCDIR)" \
                  --define='__GECO_BINDIR__'="$(BINDIR)" \
                  --define='__GECO_SBINDIR__'="$(SBINDIR)"

#

default: $(TARGETS)

%.conf: %.conf.in
	m4 $(M4_DEFINES) $< > $@

%.sh: %.sh.in
	m4 $(M4_DEFINES) $< > $@

%: %.in
	m4 $(M4_DEFINES) $< > $@

clean::
	$(RM) -f $(TARGETS)

install: $(addprefix $(ETCDIR)/,$(TARGETS))

$(ETCDIR)/%: %
	cp $< $@
	chmod 0664 $@


