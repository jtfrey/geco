#
#
#

-include ../Makefile.inc

TARGETS		= geco_prolog

#

default: $(TARGETS)

%: %.in
	m4 $(M4_DEFINES) $< > $@

clean::
	$(RM) -f $(TARGETS)

install: $(addprefix $(SBINDIR)/,$(TARGETS))

$(SBINDIR)/%: %
	cp $< $@
	chmod 0775 $@


