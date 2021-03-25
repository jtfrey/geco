#
# Default values used in many of the GECO shell scripts
#

CGROUP_PREFIX="/cgroup"

GECO_STATEDIR="/opt/geco"

GECO_BINDIR="/opt/shared/univa/GECO/bin"
GECO_ETCDIR="/opt/shared/univa/GECO/etc"
GECO_SBINDIR="/opt/shared/univa/GECO/sbin"

GECO_RSRCINFO="${GECO_BINDIR}/geco-rsrcinfo"
GECO_BOOTMIC="${GECO_SBINDIR}/geco_bootmic"
GECO_RESETMIC="${GECO_SBINDIR}/geco_resetmic"
GECO_CGROUPCLEANUP="${GECO_SBINDIR}/geco_cgroupcleanup"

SSH_OPTIONS="-4ankx -enone -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oConnectTimeout=300 -oConnectionAttempts=5 -oBatchMode=yes"

