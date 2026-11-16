#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/reboot.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define GPIO_PATH             "/sys/class/gpio/gpio9/value"
#define LOG_PATH              "/tmp/telnet_btn.log"
#define LIB_RTSP_ON           "/usr/lib/librtsp_cam.so"
#define LIB_RTSP_OFF          "/usr/lib/librtsp_cam.so.off"
#define TELNET_TAP_COUNT      3
#define TOGGLE_MODE_TAP_COUNT 5
#define TAP_MAX_HOLD_MS       800ULL
#define TAP_GAP_MS            1000ULL
#define TAP_WINDOW_MS         6000ULL
#define POLL_US               40000

static void tlog(const char *fmt, ...) {
    FILE *f = fopen(LOG_PATH, "a");
    if (!f) return;
    if (ftell(f) > 32768L) {
        FILE *nf = freopen(LOG_PATH, "w", f);
        if (nf) f = nf;
    }
    va_list ap;
    va_start(ap, fmt);
    vfprintf(f, fmt, ap);
    va_end(ap);
    fclose(f);
}

#ifndef UNIT_TEST
/* Return monotonic clock in milliseconds so NTP updates do not skew tap windows. */
static uint64_t now_mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000L);
}

/* Open/read/close sysfs GPIO each poll so Linux 3.10 returns live pin state. */
static int read_gpio_pressed(const char *path, int *out_pressed) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    char ch = 0;
    ssize_t n = read(fd, &ch, 1);
    close(fd);
    if (n != 1) return -1;
    *out_pressed = (ch == '0');
    return 0;
}

static void toggle_telnetd(void) {
    int rc = system("/usr/bin/killall telnetd 2>/dev/null");
    if (rc != -1 && WIFEXITED(rc) && WEXITSTATUS(rc) == 0) {
        tlog("[telnet_btn] telnetd disabled via 3x PAIR\n");
        return;
    }
    rc = system("mount -t devpts devpts /dev/pts 2>/dev/null; /usr/sbin/telnetd -l /bin/sh");
    tlog("[telnet_btn] telnetd enabled via 3x PAIR (rc=%d)\n", rc);
}
#endif

/*
 * Atomically toggle /usr/lib/librtsp_cam.so <-> .off so /etc/init.d/rcS boots
 * either stock or RTSP-preloaded vt_cam on the next boot without wiping anything.
 * Returns 1 if switched to stock, 2 if switched to rtsp, or -1 on error.
 */
static int toggle_rtsp_preload(const char *root_prefix) {
    char p_on[256], p_off[256];
    snprintf(p_on, sizeof(p_on), "%s%s", root_prefix, LIB_RTSP_ON);
    snprintf(p_off, sizeof(p_off), "%s%s", root_prefix, LIB_RTSP_OFF);

    if (access(p_on, F_OK) == 0) {
        if (rename(p_on, p_off) != 0) {
            tlog("[telnet_btn] 5x PAIR rename %s -> %s failed errno=%d\n", p_on, p_off, errno);
            return -1;
        }
        sync();
        sync();
        return 1;
    }
    if (access(p_off, F_OK) == 0) {
        if (rename(p_off, p_on) != 0) {
            tlog("[telnet_btn] 5x PAIR rename %s -> %s failed errno=%d\n", p_off, p_on, errno);
            return -1;
        }
        sync();
        sync();
        return 2;
    }
    tlog("[telnet_btn] 5x PAIR: neither %s nor %s exists\n", p_on, p_off);
    return -1;
}

#ifndef UNIT_TEST
static void toggle_mode_and_reboot(void) {
    int mode = toggle_rtsp_preload("");
    if (mode < 0) return;
    tlog("[telnet_btn] 5x PAIR: switched to %s boot mode, rebooting\n",
         (mode == 1) ? "stock" : "rtsp");
    int rc = system("/sbin/reboot");
    (void)rc;
    sleep(2);
    reboot(RB_AUTOBOOT);
}
#endif

typedef enum {
    BTN_ACT_NONE = 0,
    BTN_ACT_TOGGLE_TELNET,
    BTN_ACT_TOGGLE_MODE
} btn_action_t;

typedef struct {
    int was_pressed;
    uint64_t press_ms;
    uint64_t first_tap_ms;
    uint64_t last_tap_ms;
    int tap_count;
} btn_state_t;

static btn_action_t btn_step(btn_state_t *s, int pressed, uint64_t t) {
    btn_action_t act = BTN_ACT_NONE;

    if (!pressed && s->tap_count > 0 &&
        ((t - s->last_tap_ms) > TAP_GAP_MS || (t - s->first_tap_ms) > TAP_WINDOW_MS)) {
        if (s->tap_count == TELNET_TAP_COUNT) {
            act = BTN_ACT_TOGGLE_TELNET;
        }
        s->tap_count = 0;
    }

    if (pressed && !s->was_pressed) {
        s->press_ms = t;
    } else if (!pressed && s->was_pressed) {
        uint64_t held = t - s->press_ms;
        if (held < TAP_MAX_HOLD_MS) {
            if (s->tap_count == 0 ||
                (t - s->last_tap_ms) > TAP_GAP_MS ||
                (t - s->first_tap_ms) > TAP_WINDOW_MS) {
                s->first_tap_ms = t;
                s->tap_count = 1;
            } else {
                s->tap_count++;
            }
            s->last_tap_ms = t;
            tlog("[telnet_btn] tap %d (%ums)\n", s->tap_count, (unsigned)held);
            if (s->tap_count >= TOGGLE_MODE_TAP_COUNT) {
                s->tap_count = 0;
                act = BTN_ACT_TOGGLE_MODE;
            }
        } else {
            s->tap_count = 0;
        }
    }

    s->was_pressed = pressed;
    return act;
}

#ifdef UNIT_TEST
#include <assert.h>
static void write_dummy(const char *p, const char *s) {
    FILE *f = fopen(p, "w");
    assert(f != NULL);
    fputs(s, f);
    fclose(f);
}

int main(void) {
    /* Test 1: 3 quick taps + >1s pause -> TOGGLE_TELNET */
    btn_state_t st = {0};
    uint64_t t = 1000;
    int i;
    for (i = 0; i < 3; i++) {
        assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 150;
        assert(btn_step(&st, 0, t) == BTN_ACT_NONE); t += 250;
    }
    assert(btn_step(&st, 0, t + 1000) == BTN_ACT_TOGGLE_TELNET);

    /* Test 2: 5 quick taps -> TOGGLE_MODE immediately on 5th release */
    memset(&st, 0, sizeof(st));
    t = 10000;
    for (i = 0; i < 4; i++) {
        assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 150;
        assert(btn_step(&st, 0, t) == BTN_ACT_NONE); t += 250;
    }
    assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 150;
    assert(btn_step(&st, 0, t) == BTN_ACT_TOGGLE_MODE);

    /* Test 3: 4 quick taps + pause -> NONE */
    memset(&st, 0, sizeof(st));
    t = 20000;
    for (i = 0; i < 4; i++) {
        assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 150;
        assert(btn_step(&st, 0, t) == BTN_ACT_NONE); t += 250;
    }
    assert(btn_step(&st, 0, t + 1200) == BTN_ACT_NONE);

    /* Test 4: Long hold resets tap count */
    memset(&st, 0, sizeof(st));
    t = 30000;
    for (i = 0; i < 2; i++) {
        assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 150;
        assert(btn_step(&st, 0, t) == BTN_ACT_NONE); t += 250;
    }
    assert(btn_step(&st, 1, t) == BTN_ACT_NONE); t += 1000;
    assert(btn_step(&st, 0, t) == BTN_ACT_NONE);
    assert(st.tap_count == 0);

    /* Test 5: Reversible rename toggle in sandbox */
    assert(system("rm -rf /tmp/tbtn_test && mkdir -p /tmp/tbtn_test/usr/lib") == 0);
    write_dummy("/tmp/tbtn_test/usr/lib/librtsp_cam.so", "dummy-so");
    assert(toggle_rtsp_preload("/tmp/tbtn_test") == 1);
    assert(access("/tmp/tbtn_test/usr/lib/librtsp_cam.so", F_OK) != 0);
    assert(access("/tmp/tbtn_test/usr/lib/librtsp_cam.so.off", F_OK) == 0);
    assert(toggle_rtsp_preload("/tmp/tbtn_test") == 2);
    assert(access("/tmp/tbtn_test/usr/lib/librtsp_cam.so", F_OK) == 0);
    assert(access("/tmp/tbtn_test/usr/lib/librtsp_cam.so.off", F_OK) != 0);
    assert(system("rm -rf /tmp/tbtn_test") == 0);
    printf("ALL UNIT TESTS PASSED\n");
    return 0;
}
#else
int main(void) {
    btn_state_t st;
    memset(&st, 0, sizeof(st));

    tlog("[telnet_btn] started pid=%d (3x=telnet, 5x=toggle stock/rtsp)\n", (int)getpid());
    for (;;) {
        int pressed = 0;
        if (read_gpio_pressed(GPIO_PATH, &pressed) < 0) {
            usleep(500000);
            continue;
        }

        uint64_t t = now_mono_ms();
        btn_action_t act = btn_step(&st, pressed, t);
        if (act == BTN_ACT_TOGGLE_TELNET) {
            toggle_telnetd();
        } else if (act == BTN_ACT_TOGGLE_MODE) {
            toggle_mode_and_reboot();
        }
        usleep(POLL_US);
    }
    return 0;
}
#endif
