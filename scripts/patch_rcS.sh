#!/bin/sh
# Idempotently patches stock /etc/init.d/rcS on the camera to:
#   1) Start /usr/sbin/telnet_btn (3x PAIR toggles telnetd; default OFF on boot)
#   2) Wrap /home/root/boot_script with /etc/init.d/app_launch.sh when librtsp_cam.so exists
set -e

RCS="${1:-/etc/init.d/rcS}"
STOCK="${RCS}.stock"

if grep -q "/usr/sbin/telnet_btn" "$RCS"; then
    echo "$RCS already patched"
    exit 0
fi

[ -f "$STOCK" ] || cp "$RCS" "$STOCK"

awk '
BEGIN { q = sprintf("%c", 39); in_telnet = 0 }
/^# Start telnet daemon/ {
    in_telnet = 1
    print "# Start PAIR-button telnet toggle daemon (default OFF on boot)"
    print "mount -t devpts devpts /dev/pts 2>/dev/null"
    print "/usr/sbin/telnet_btn </dev/null >/dev/null 2>&1 &"
    next
}
in_telnet {
    if ($0 ~ /telnetd|devpts/) next
    in_telnet = 0
}
/^\. \/home\/root\/boot_script NODEBUG/ {
    print "if [ -f /etc/test_preload.so ] || [ -f /usr/lib/librtsp_cam.so ]; then"
    print "    sed " q "s|./run_application.sh.*|/etc/init.d/app_launch.sh \\&|" q " /home/root/boot_script > /tmp/boot_wrap.sh"
    print "    . /tmp/boot_wrap.sh NODEBUG"
    print "else"
    print "    . /home/root/boot_script NODEBUG"
    print "fi"
    next
}
{ print }
' "$RCS" > "${RCS}.new"

chmod 755 "${RCS}.new"
mv "${RCS}.new" "$RCS"
sync
echo "Patched $RCS (backup saved at $STOCK)"
