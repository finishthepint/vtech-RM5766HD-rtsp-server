#!/bin/sh
echo "run app_launch"
sysctl -w vm.swappiness=80
/bin/gpiotool A 15 2mA
cd /home/root
if [ -f /etc/test_preload.so ]; then
    mv /etc/test_preload.so /tmp/test_preload.so
    LD_PRELOAD=/tmp/test_preload.so /home/root/vt_cam </dev/null >/tmp/vt_cam_out.log 2>&1 &
elif [ -f /usr/lib/librtsp_cam.so ]; then
    LD_PRELOAD=/usr/lib/librtsp_cam.so /home/root/vt_cam 2>/dev/null 1>/dev/null
    echo "reboot......."
    reboot
else
    exec /home/root/run_application.sh NODEBUG
fi
