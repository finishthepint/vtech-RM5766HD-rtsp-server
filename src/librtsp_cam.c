#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/cachectl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define RTSP_PORT          554
#define VIRTUAL_SID        7
#define VIRTUAL_AVINDEX    107
#define MAX_CLIENTS        4
#define CLIENT_RING_SIZE   (512 * 1024)
#define RX_BUF_SIZE        4096
#define MAX_RTP_PAYLOAD    1400
#define PRIVACY_FLAG_ADDR  0x58e9cd
#define VIDEO_RES_ADDR     0x58e934
#define OPEN_CH_JAL_ADDR   0x41cb28
#define OPEN_CH_ARG_ADDR   0x41cb2c
#define STOP_STREAM_ARG_ADDR 0x41b0b8

static int g_is_vt_cam = 0;

/* =========================================================================
 * Real ThroughTek function pointers (resolved via RTLD_NEXT / dlopen)
 * ========================================================================= */
static int (*real_IOTC_Set_Max_Session_Number)(unsigned int num) = NULL;
static int (*real_IOTC_Listen)(unsigned int timeout_ms) = NULL;
static int (*real_IOTC_Session_Check_Ex)(int sid, void *s_info) = NULL;
static void (*real_IOTC_Session_Close)(int sid) = NULL;
static int (*real_avServStartEx)(const void *in, void *out) = NULL;
static void (*real_avServStop)(int avIndex) = NULL;
static int (*real_avServSetResendSize)(int avIndex, unsigned int size) = NULL;
static int (*real_avServResetBuffer)(int avIndex, int target, unsigned int timeout_ms) = NULL;
static float (*real_avResendBufUsageRate)(int avIndex) = NULL;
static int (*real_avRecvIOCtrl)(int avIndex, unsigned int *pioType, char *buf, int maxLen, unsigned int timeout_ms) = NULL;
static int (*real_avSendIOCtrl)(int avIndex, unsigned int ioType, const char *buf, int len) = NULL;
static int (*real_avSendFrameData)(int avIndex, const char *data, int size, const void *info, int infoSize) = NULL;
static int (*real_avSendAudioData)(int avIndex, const char *data, int size, const void *info, int infoSize) = NULL;
static int (*real_avRecvAudioData)(int avIndex, char *buf, int maxLen, void *info, unsigned int *pFrameIdx) = NULL;

/* =========================================================================
 * Lightweight logging
 * ========================================================================= */
static FILE *g_log = NULL;
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;

static void rlog(const char *fmt, ...) {
    pthread_mutex_lock(&g_log_mu);
    if (!g_log) g_log = fopen("/tmp/rtsp_cam.log", "a");
    if (g_log) {
        va_list ap;
        va_start(ap, fmt);
        vfprintf(g_log, fmt, ap);
        va_end(ap);
        fflush(g_log);
    }
    pthread_mutex_unlock(&g_log_mu);
}

/* =========================================================================
 * Base64 encoder for SPS/PPS sprop-parameter-sets
 * ========================================================================= */
static const char B64_TBL[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int b64_encode(const uint8_t *in, int len, char *out, int out_max) {
    int i = 0, o = 0;
    for (i = 0; i < len && o + 5 <= out_max; i += 3) {
        int n = len - i;
        uint32_t v = ((uint32_t)in[i] << 16)
                   | ((n > 1 ? (uint32_t)in[i + 1] : 0) << 8)
                   | (n > 2 ? (uint32_t)in[i + 2] : 0);
        out[o++] = B64_TBL[(v >> 18) & 0x3F];
        out[o++] = B64_TBL[(v >> 12) & 0x3F];
        out[o++] = (n > 1) ? B64_TBL[(v >> 6) & 0x3F] : '=';
        out[o++] = (n > 2) ? B64_TBL[v & 0x3F] : '=';
    }
    out[o] = '\0';
    return o;
}

/* =========================================================================
 * H.264 Parameter Set Cache
 * ========================================================================= */
static pthread_mutex_t g_ps_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_ps_cv = PTHREAD_COND_INITIALIZER;
static uint8_t g_sps[256];
static int     g_sps_len = 0;
static uint8_t g_pps[256];
static int     g_pps_len = 0;
static char    g_sps_b64[384];
static char    g_pps_b64[384];
static char    g_prof_id[16] = "640033";

/* =========================================================================
 * Client structures & bounded ring buffer
 * ========================================================================= */
typedef struct {
    int in_use;
    int fd;
    uint32_t session_id;
    struct sockaddr_in peer_addr;
    char cam_ip[32];

    /* RTSP RX text/interleaved parser state */
    uint8_t rx_buf[RX_BUF_SIZE];
    int rx_len;

    /* Media setup flags */
    int v_setup;
    int v_is_tcp;
    int v_ch_rtp;
    int v_ch_rtcp;
    int v_udp_rtp_fd;
    int v_udp_rtcp_fd;
    uint16_t v_udp_peer_rtp_port;
    uint16_t v_udp_srv_rtp_port;
    uint16_t v_seq;
    uint32_t v_ssrc;

    int a_setup;
    int a_is_tcp;
    int a_ch_rtp;
    int a_ch_rtcp;
    int a_udp_rtp_fd;
    int a_udp_rtcp_fd;
    uint16_t a_udp_peer_rtp_port;
    uint16_t a_udp_srv_rtp_port;
    uint16_t a_seq;
    uint32_t a_ssrc;

    volatile int playing;
    volatile int need_idr;

    /* Outbound TCP RTSP response + media ring buffer */
    pthread_mutex_t tx_mu;
    uint8_t *tx_ring; /* allocated on demand (CLIENT_RING_SIZE) */
    uint32_t tx_head; /* write offset % CLIENT_RING_SIZE */
    uint32_t tx_tail; /* read offset % CLIENT_RING_SIZE */
    uint32_t tx_used;
} rtsp_client_t;

static rtsp_client_t g_clients[MAX_CLIENTS];
static pthread_mutex_t g_clients_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_wakeup_pipe[2] = {-1, -1};
static uint32_t g_next_session = 0x56540001u;

static void wake_rtsp_loop(void) {
    if (g_wakeup_pipe[1] >= 0) {
        char b = 1;
        ssize_t r = write(g_wakeup_pipe[1], &b, 1);
        (void)r;
    }
}

static void set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl >= 0) fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static uint32_t ring_free(const rtsp_client_t *c) {
    return c->tx_ring ? (CLIENT_RING_SIZE - c->tx_used) : 0;
}

static void ring_write_raw(rtsp_client_t *c, const void *src, uint32_t len) {
    const uint8_t *p = (const uint8_t *)src;
    uint32_t first = CLIENT_RING_SIZE - c->tx_head;
    if (first > len) first = len;
    memcpy(c->tx_ring + c->tx_head, p, first);
    if (len > first) memcpy(c->tx_ring, p + first, len - first);
    c->tx_head = (c->tx_head + len) % CLIENT_RING_SIZE;
    c->tx_used += len;
}

static int ring_enqueue_tcp_frame(rtsp_client_t *c, int ch, const uint8_t *rtp, int rtp_len) {
    uint32_t need = 4u + (uint32_t)rtp_len;
    if (ring_free(c) < need) return -1;
    uint8_t hdr[4];
    hdr[0] = '$';
    hdr[1] = (uint8_t)ch;
    hdr[2] = (uint8_t)((rtp_len >> 8) & 0xFF);
    hdr[3] = (uint8_t)(rtp_len & 0xFF);
    ring_write_raw(c, hdr, 4);
    ring_write_raw(c, rtp, (uint32_t)rtp_len);
    return 0;
}

static int queue_rtsp_reply(rtsp_client_t *c, const char *buf, int len) {
    int ret = -1;
    pthread_mutex_lock(&c->tx_mu);
    if (c->tx_ring && ring_free(c) >= (uint32_t)len) {
        ring_write_raw(c, buf, (uint32_t)len);
        ret = 0;
    }
    pthread_mutex_unlock(&c->tx_mu);
    return ret;
}

static int send_udp_rtp(int fd, const struct sockaddr_in *peer, uint16_t port, const uint8_t *rtp, int len) {
    if (fd < 0 || port == 0) return -1;
    struct sockaddr_in dst = *peer;
    dst.sin_port = htons(port);
    ssize_t s = sendto(fd, rtp, (size_t)len, MSG_DONTWAIT | MSG_NOSIGNAL,
                       (const struct sockaddr *)&dst, sizeof(dst));
    return (s == len) ? 0 : -1;
}

/* =========================================================================
 * RTP packet construction & non-blocking fan-out
 * ========================================================================= */
static void fill_rtp_hdr(uint8_t *h, uint8_t pt, int marker, uint16_t seq, uint32_t ts, uint32_t ssrc) {
    h[0] = 0x80;
    h[1] = (uint8_t)((marker ? 0x80 : 0x00) | (pt & 0x7F));
    h[2] = (uint8_t)((seq >> 8) & 0xFF);
    h[3] = (uint8_t)(seq & 0xFF);
    h[4] = (uint8_t)((ts >> 24) & 0xFF);
    h[5] = (uint8_t)((ts >> 16) & 0xFF);
    h[6] = (uint8_t)((ts >> 8) & 0xFF);
    h[7] = (uint8_t)(ts & 0xFF);
    h[8] = (uint8_t)((ssrc >> 24) & 0xFF);
    h[9] = (uint8_t)((ssrc >> 16) & 0xFF);
    h[10] = (uint8_t)((ssrc >> 8) & 0xFF);
    h[11] = (uint8_t)(ssrc & 0xFF);
}

typedef struct {
    const uint8_t *ptr;
    int len;
    uint8_t type;
} nal_slice_t;

static int parse_annexb(const uint8_t *data, int size, nal_slice_t *out, int max_nals) {
    int count = 0;
    int i = 0;
    int nal_start = -1;

    while (i + 2 < size) {
        int sc_len = 0;
        if (i + 3 < size && data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 0 && data[i + 3] == 1) {
            sc_len = 4;
        } else if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1) {
            sc_len = 3;
        }
        if (sc_len > 0) {
            if (nal_start >= 0 && i > nal_start && count < max_nals) {
                int len = i - nal_start;
                while (len > 0 && data[nal_start + len - 1] == 0) len--;
                if (len > 0) {
                    out[count].ptr = data + nal_start;
                    out[count].len = len;
                    out[count].type = data[nal_start] & 0x1F;
                    count++;
                }
            }
            i += sc_len;
            nal_start = i;
            continue;
        }
        i++;
    }
    if (nal_start >= 0 && size > nal_start && count < max_nals) {
        int len = size - nal_start;
        if (len > 0) {
            out[count].ptr = data + nal_start;
            out[count].len = len;
            out[count].type = data[nal_start] & 0x1F;
            count++;
        }
    }
    return count;
}

static void cache_param_sets(const nal_slice_t *nals, int n) {
    int updated = 0;
    int i;
    for (i = 0; i < n; i++) {
        if (nals[i].type == 7 && nals[i].len > 0 && nals[i].len <= (int)sizeof(g_sps)) {
            pthread_mutex_lock(&g_ps_mu);
            if (g_sps_len != nals[i].len || memcmp(g_sps, nals[i].ptr, (size_t)nals[i].len) != 0) {
                memcpy(g_sps, nals[i].ptr, (size_t)nals[i].len);
                g_sps_len = nals[i].len;
                b64_encode(g_sps, g_sps_len, g_sps_b64, sizeof(g_sps_b64));
                if (g_sps_len >= 4) {
                    sprintf(g_prof_id, "%02X%02X%02X", g_sps[1], g_sps[2], g_sps[3]);
                }
                updated = 1;
            }
            pthread_mutex_unlock(&g_ps_mu);
        } else if (nals[i].type == 8 && nals[i].len > 0 && nals[i].len <= (int)sizeof(g_pps)) {
            pthread_mutex_lock(&g_ps_mu);
            if (g_pps_len != nals[i].len || memcmp(g_pps, nals[i].ptr, (size_t)nals[i].len) != 0) {
                memcpy(g_pps, nals[i].ptr, (size_t)nals[i].len);
                g_pps_len = nals[i].len;
                b64_encode(g_pps, g_pps_len, g_pps_b64, sizeof(g_pps_b64));
                updated = 1;
            }
            pthread_mutex_unlock(&g_ps_mu);
        }
    }
    if (updated) {
        pthread_mutex_lock(&g_ps_mu);
        if (g_sps_len > 0 && g_pps_len > 0) {
            pthread_cond_broadcast(&g_ps_cv);
        }
        pthread_mutex_unlock(&g_ps_mu);
    }
}

static uint32_t estimate_nal_rtp_tcp_bytes(const uint8_t *nal, int len) {
    (void)nal;
    if (len <= MAX_RTP_PAYLOAD) return (uint32_t)(4 + 12 + len);
    int rem = len - 1;
    int chunk = MAX_RTP_PAYLOAD - 2;
    int pkts = (rem + chunk - 1) / chunk;
    return (uint32_t)((4 + 12 + 2) * pkts + rem);
}

static int send_nal_to_client(rtsp_client_t *c, const uint8_t *nal, int len, uint32_t rtp_ts, int last_nal_in_au) {
    uint8_t pkt[12 + 2 + MAX_RTP_PAYLOAD];
    if (len <= 0) return 0;

    if (len <= MAX_RTP_PAYLOAD) {
        fill_rtp_hdr(pkt, 96, last_nal_in_au, c->v_seq++, rtp_ts, c->v_ssrc);
        memcpy(pkt + 12, nal, (size_t)len);
        if (c->v_is_tcp) {
            return ring_enqueue_tcp_frame(c, c->v_ch_rtp, pkt, 12 + len);
        } else {
            return send_udp_rtp(c->v_udp_rtp_fd, &c->peer_addr, c->v_udp_peer_rtp_port, pkt, 12 + len);
        }
    }

    uint8_t nri = nal[0] & 0xE0;
    uint8_t ntype = nal[0] & 0x1F;
    const uint8_t *p = nal + 1;
    int rem = len - 1;
    int chunk_max = MAX_RTP_PAYLOAD - 2;
    int first = 1;

    while (rem > 0) {
        int chunk = (rem > chunk_max) ? chunk_max : rem;
        int is_last = (rem - chunk == 0);
        int marker = (is_last && last_nal_in_au) ? 1 : 0;

        fill_rtp_hdr(pkt, 96, marker, c->v_seq++, rtp_ts, c->v_ssrc);
        pkt[12] = nri | 28; /* FU-A indicator */
        pkt[13] = (uint8_t)((first ? 0x80 : 0) | (is_last ? 0x40 : 0) | ntype);
        memcpy(pkt + 14, p, (size_t)chunk);

        if (c->v_is_tcp) {
            if (ring_enqueue_tcp_frame(c, c->v_ch_rtp, pkt, 14 + chunk) != 0) return -1;
        } else {
            send_udp_rtp(c->v_udp_rtp_fd, &c->peer_addr, c->v_udp_peer_rtp_port, pkt, 14 + chunk);
        }
        p += chunk;
        rem -= chunk;
        first = 0;
    }
    return 0;
}

static void fanout_video_frame(const uint8_t *data, int size, uint8_t flags, uint32_t ts_ms) {
    nal_slice_t nals[32];
    int n = parse_annexb(data, size, nals, 32);
    if (n <= 0) return;

    cache_param_sets(nals, n);

    int has_idr = (flags & 1) != 0;
    int has_sps = 0, has_pps = 0;
    int i;
    for (i = 0; i < n; i++) {
        if (nals[i].type == 5) has_idr = 1;
        if (nals[i].type == 7) has_sps = 1;
        if (nals[i].type == 8) has_pps = 1;
    }

    uint32_t rtp_ts = (uint32_t)((uint64_t)ts_ms * 90ULL);
    int need_wake = 0;

    uint8_t sps_copy[256], pps_copy[256];
    int sps_len = 0, pps_len = 0;
    if (has_idr && (!has_sps || !has_pps)) {
        pthread_mutex_lock(&g_ps_mu);
        if (g_sps_len > 0 && g_pps_len > 0) {
            memcpy(sps_copy, g_sps, (size_t)g_sps_len);
            sps_len = g_sps_len;
            memcpy(pps_copy, g_pps, (size_t)g_pps_len);
            pps_len = g_pps_len;
        }
        pthread_mutex_unlock(&g_ps_mu);
    }

    uint32_t frame_tcp_bytes = 0;
    for (i = 0; i < n; i++) {
        frame_tcp_bytes += estimate_nal_rtp_tcp_bytes(nals[i].ptr, nals[i].len);
    }

    pthread_mutex_lock(&g_clients_mu);
    for (i = 0; i < MAX_CLIENTS; i++) {
        rtsp_client_t *c = &g_clients[i];
        if (!c->in_use || !c->playing || !c->v_setup) continue;

        if (c->need_idr && !has_idr) continue;

        uint32_t extra = 0;
        int prepend_ps = (has_idr && (!has_sps || !has_pps) && sps_len > 0 && pps_len > 0);
        if (prepend_ps) {
            extra += estimate_nal_rtp_tcp_bytes(sps_copy, sps_len);
            extra += estimate_nal_rtp_tcp_bytes(pps_copy, pps_len);
        }

        if (c->v_is_tcp) {
            pthread_mutex_lock(&c->tx_mu);
            if (!c->tx_ring || ring_free(c) < frame_tcp_bytes + extra) {
                /* Drop whole frame cleanly and wait for next IDR to avoid corrupting stream */
                c->need_idr = 1;
                pthread_mutex_unlock(&c->tx_mu);
                continue;
            }
            if (prepend_ps) {
                send_nal_to_client(c, sps_copy, sps_len, rtp_ts, 0);
                send_nal_to_client(c, pps_copy, pps_len, rtp_ts, 0);
            }
            int j;
            for (j = 0; j < n; j++) {
                send_nal_to_client(c, nals[j].ptr, nals[j].len, rtp_ts, (j == n - 1));
            }
            c->need_idr = 0;
            need_wake = 1;
            pthread_mutex_unlock(&c->tx_mu);
        } else {
            if (prepend_ps) {
                send_nal_to_client(c, sps_copy, sps_len, rtp_ts, 0);
                send_nal_to_client(c, pps_copy, pps_len, rtp_ts, 0);
            }
            int j;
            for (j = 0; j < n; j++) {
                send_nal_to_client(c, nals[j].ptr, nals[j].len, rtp_ts, (j == n - 1));
            }
            c->need_idr = 0;
        }
    }
    pthread_mutex_unlock(&g_clients_mu);

    if (need_wake) wake_rtsp_loop();
}

static void fanout_audio_frame(const uint8_t *data, int size, uint32_t ts_ms) {
    if (size <= 0 || size > MAX_RTP_PAYLOAD) return;
    uint32_t rtp_ts = (uint32_t)((uint64_t)ts_ms * 8ULL);
    uint8_t pkt[12 + MAX_RTP_PAYLOAD];
    int need_wake = 0;
    int i;

    pthread_mutex_lock(&g_clients_mu);
    for (i = 0; i < MAX_CLIENTS; i++) {
        rtsp_client_t *c = &g_clients[i];
        if (!c->in_use || !c->playing || !c->a_setup) continue;
        /* Hold audio until video keyframe has started if video is also requested */
        if (c->v_setup && c->need_idr) continue;

        fill_rtp_hdr(pkt, 8 /* PCMA */, 0, c->a_seq++, rtp_ts, c->a_ssrc);
        memcpy(pkt + 12, data, (size_t)size);

        if (c->a_is_tcp) {
            pthread_mutex_lock(&c->tx_mu);
            if (c->tx_ring && ring_free(c) >= (uint32_t)(4 + 12 + size)) {
                ring_enqueue_tcp_frame(c, c->a_ch_rtp, pkt, 12 + size);
                need_wake = 1;
            }
            pthread_mutex_unlock(&c->tx_mu);
        } else {
            send_udp_rtp(c->a_udp_rtp_fd, &c->peer_addr, c->a_udp_peer_rtp_port, pkt, 12 + size);
        }
    }
    pthread_mutex_unlock(&g_clients_mu);

    if (need_wake) wake_rtsp_loop();
}

/* =========================================================================
 * RTSP/1.0 server (single poll() thread owning accept, read, and TCP writes)
 * ========================================================================= */
static int create_udp_pair(int *rtp_fd, int *rtcp_fd, uint16_t *srv_rtp_port) {
    int base_port;
    for (base_port = 30000; base_port < 31000; base_port += 2) {
        int s1 = socket(AF_INET, SOCK_DGRAM, 0);
        int s2 = socket(AF_INET, SOCK_DGRAM, 0);
        if (s1 < 0 || s2 < 0) {
            if (s1 >= 0) close(s1);
            if (s2 >= 0) close(s2);
            return -1;
        }
        int one = 1;
        setsockopt(s1, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(s2, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        struct sockaddr_in a1, a2;
        memset(&a1, 0, sizeof(a1));
        memset(&a2, 0, sizeof(a2));
        a1.sin_family = AF_INET;
        a1.sin_addr.s_addr = INADDR_ANY;
        a1.sin_port = htons((uint16_t)base_port);
        a2.sin_family = AF_INET;
        a2.sin_addr.s_addr = INADDR_ANY;
        a2.sin_port = htons((uint16_t)(base_port + 1));
        if (bind(s1, (struct sockaddr *)&a1, sizeof(a1)) == 0 &&
            bind(s2, (struct sockaddr *)&a2, sizeof(a2)) == 0) {
            set_nonblock(s1);
            set_nonblock(s2);
            *rtp_fd = s1;
            *rtcp_fd = s2;
            *srv_rtp_port = (uint16_t)base_port;
            return 0;
        }
        close(s1);
        close(s2);
    }
    return -1;
}

static void update_rtsp_demand(int want);

static void close_client(rtsp_client_t *c) {
    int remaining = 0;
    int i;
    pthread_mutex_lock(&g_clients_mu);
    c->playing = 0;
    c->in_use = 0;
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i].in_use) remaining++;
    }
    pthread_mutex_unlock(&g_clients_mu);

    if (c->fd >= 0) { close(c->fd); c->fd = -1; }
    if (c->v_udp_rtp_fd >= 0) { close(c->v_udp_rtp_fd); c->v_udp_rtp_fd = -1; }
    if (c->v_udp_rtcp_fd >= 0) { close(c->v_udp_rtcp_fd); c->v_udp_rtcp_fd = -1; }
    if (c->a_udp_rtp_fd >= 0) { close(c->a_udp_rtp_fd); c->a_udp_rtp_fd = -1; }
    if (c->a_udp_rtcp_fd >= 0) { close(c->a_udp_rtcp_fd); c->a_udp_rtcp_fd = -1; }

    pthread_mutex_lock(&c->tx_mu);
    if (c->tx_ring) { free(c->tx_ring); c->tx_ring = NULL; }
    c->tx_head = c->tx_tail = c->tx_used = 0;
    pthread_mutex_unlock(&c->tx_mu);

    if (remaining == 0) update_rtsp_demand(0);
}

static const char *find_hdr_ci(const char *req, const char *name) {
    size_t nlen = strlen(name);
    const char *p = req;
    while (*p) {
        const char *eol = strstr(p, "\r\n");
        if (!eol) break;
        if (eol == p) break;
        if ((size_t)(eol - p) > nlen && strncasecmp(p, name, nlen) == 0 && p[nlen] == ':') {
            p += nlen + 1;
            while (p < eol && (*p == ' ' || *p == '\t')) p++;
            return p;
        }
        p = eol + 2;
    }
    return NULL;
}

static void extract_hdr_line(const char *val, char *out, int out_max) {
    int i = 0;
    while (val && val[i] && val[i] != '\r' && val[i] != '\n' && i + 1 < out_max) {
        out[i] = val[i];
        i++;
    }
    out[i] = '\0';
}

static void handle_rtsp_request(rtsp_client_t *c, const char *req) {
    char method[32] = {0}, url[256] = {0}, cseq[32] = "0";
    if (sscanf(req, "%31s %255s", method, url) != 2) return;

    const char *hc = find_hdr_ci(req, "CSeq");
    if (hc) extract_hdr_line(hc, cseq, sizeof(cseq));

    char resp[2048];
    int rlen = 0;

    rlog("[RTSP] fd=%d %s %s CSeq=%s\n", c->fd, method, url, cseq);

    if (strcmp(method, "OPTIONS") == 0) {
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nPublic: OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, GET_PARAMETER, TEARDOWN\r\n\r\n",
            cseq);
        queue_rtsp_reply(c, resp, rlen);
    } else if (strcmp(method, "DESCRIBE") == 0) {
        update_rtsp_demand(1);
        char sps_b64[384], pps_b64[384], prof[16];
        pthread_mutex_lock(&g_ps_mu);
        if (g_sps_len == 0 || g_pps_len == 0) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 4;
            pthread_cond_timedwait(&g_ps_cv, &g_ps_mu, &ts);
        }
        strcpy(sps_b64, g_sps_len ? g_sps_b64 : "");
        strcpy(pps_b64, g_pps_len ? g_pps_b64 : "");
        strcpy(prof, g_prof_id);
        pthread_mutex_unlock(&g_ps_mu);

        if (sps_b64[0] == '\0' || pps_b64[0] == '\0') {
            rlen = snprintf(resp, sizeof(resp),
                "RTSP/1.0 503 Service Unavailable\r\nCSeq: %s\r\nRetry-After: 1\r\n\r\n", cseq);
            queue_rtsp_reply(c, resp, rlen);
            return;
        }

        char sdp[1024];
        int slen = snprintf(sdp, sizeof(sdp),
            "v=0\r\n"
            "o=- %u 1 IN IP4 %s\r\n"
            "s=VTech RM5766HD Live\r\n"
            "c=IN IP4 0.0.0.0\r\n"
            "t=0 0\r\n"
            "a=control:*\r\n"
            "m=video 0 RTP/AVP 96\r\n"
            "a=rtpmap:96 H264/90000\r\n"
            "a=fmtp:96 packetization-mode=1;profile-level-id=%s;sprop-parameter-sets=%s,%s\r\n"
            "a=control:trackID=0\r\n"
            "m=audio 0 RTP/AVP 8\r\n"
            "a=rtpmap:8 PCMA/8000\r\n"
            "a=control:trackID=1\r\n",
            c->session_id, c->cam_ip, prof, sps_b64, pps_b64);

        size_t ulen = strlen(url);
        const char *slash = (ulen > 0 && url[ulen - 1] == '/') ? "" : "/";
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nContent-Type: application/sdp\r\nContent-Base: %s%s\r\nContent-Length: %d\r\n\r\n%s",
            cseq, url, slash, slen, sdp);
        queue_rtsp_reply(c, resp, rlen);
    } else if (strcmp(method, "SETUP") == 0) {
        const char *ht = find_hdr_ci(req, "Transport");
        char trans[256] = {0};
        if (ht) extract_hdr_line(ht, trans, sizeof(trans));

        int is_audio = (strstr(url, "trackID=1") != NULL || strstr(url, "track1") != NULL || strstr(url, "audio") != NULL);
        int is_tcp = (strstr(trans, "RTP/AVP/TCP") != NULL || strstr(trans, "interleaved=") != NULL);

        if (is_tcp) {
            int c0 = is_audio ? 2 : 0, c1 = is_audio ? 3 : 1;
            const char *iv = strstr(trans, "interleaved=");
            if (iv) {
                int a = 0, b = 1;
                if (sscanf(iv + 12, "%d-%d", &a, &b) == 2) { c0 = a; c1 = b; }
                else if (sscanf(iv + 12, "%d", &a) == 1) { c0 = a; c1 = a + 1; }
            }
            if (is_audio) {
                c->a_setup = 1; c->a_is_tcp = 1; c->a_ch_rtp = c0; c->a_ch_rtcp = c1;
            } else {
                c->v_setup = 1; c->v_is_tcp = 1; c->v_ch_rtp = c0; c->v_ch_rtcp = c1;
            }
            rlen = snprintf(resp, sizeof(resp),
                "RTSP/1.0 200 OK\r\nCSeq: %s\r\nTransport: RTP/AVP/TCP;unicast;interleaved=%d-%d\r\nSession: %08X;timeout=60\r\n\r\n",
                cseq, c0, c1, c->session_id);
            queue_rtsp_reply(c, resp, rlen);
        } else {
            int p0 = 0, p1 = 0;
            const char *cp = strstr(trans, "client_port=");
            if (cp) {
                if (sscanf(cp + 12, "%d-%d", &p0, &p1) == 1) p1 = p0 + 1;
            }
            if (p0 <= 0) {
                rlen = snprintf(resp, sizeof(resp), "RTSP/1.0 461 Unsupported Transport\r\nCSeq: %s\r\n\r\n", cseq);
                queue_rtsp_reply(c, resp, rlen);
                return;
            }
            int rfd = -1, cfd = -1;
            uint16_t srv_p = 0;
            if (create_udp_pair(&rfd, &cfd, &srv_p) != 0) {
                rlen = snprintf(resp, sizeof(resp), "RTSP/1.0 500 Internal Server Error\r\nCSeq: %s\r\n\r\n", cseq);
                queue_rtsp_reply(c, resp, rlen);
                return;
            }
            if (is_audio) {
                if (c->a_udp_rtp_fd >= 0) close(c->a_udp_rtp_fd);
                if (c->a_udp_rtcp_fd >= 0) close(c->a_udp_rtcp_fd);
                c->a_setup = 1; c->a_is_tcp = 0;
                c->a_udp_rtp_fd = rfd; c->a_udp_rtcp_fd = cfd;
                c->a_udp_srv_rtp_port = srv_p;
                c->a_udp_peer_rtp_port = (uint16_t)p0;
            } else {
                if (c->v_udp_rtp_fd >= 0) close(c->v_udp_rtp_fd);
                if (c->v_udp_rtcp_fd >= 0) close(c->v_udp_rtcp_fd);
                c->v_setup = 1; c->v_is_tcp = 0;
                c->v_udp_rtp_fd = rfd; c->v_udp_rtcp_fd = cfd;
                c->v_udp_srv_rtp_port = srv_p;
                c->v_udp_peer_rtp_port = (uint16_t)p0;
            }
            rlen = snprintf(resp, sizeof(resp),
                "RTSP/1.0 200 OK\r\nCSeq: %s\r\nTransport: RTP/AVP;unicast;client_port=%d-%d;server_port=%u-%u\r\nSession: %08X;timeout=60\r\n\r\n",
                cseq, p0, p1, srv_p, srv_p + 1, c->session_id);
            queue_rtsp_reply(c, resp, rlen);
        }
    } else if (strcmp(method, "PLAY") == 0) {
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nRange: npt=0.000-\r\nSession: %08X;timeout=60\r\n\r\n",
            cseq, c->session_id);
        queue_rtsp_reply(c, resp, rlen);
        c->need_idr = 1;
        c->playing = 1;
    } else if (strcmp(method, "PAUSE") == 0) {
        c->playing = 0;
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nSession: %08X\r\n\r\n", cseq, c->session_id);
        queue_rtsp_reply(c, resp, rlen);
    } else if (strcmp(method, "GET_PARAMETER") == 0 || strcmp(method, "SET_PARAMETER") == 0) {
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nSession: %08X\r\n\r\n", cseq, c->session_id);
        queue_rtsp_reply(c, resp, rlen);
    } else if (strcmp(method, "TEARDOWN") == 0) {
        c->playing = 0;
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 200 OK\r\nCSeq: %s\r\nSession: %08X\r\n\r\n", cseq, c->session_id);
        queue_rtsp_reply(c, resp, rlen);
        /* Close after flush or mark closing; client will close fd cleanly */
    } else {
        rlen = snprintf(resp, sizeof(resp),
            "RTSP/1.0 501 Not Implemented\r\nCSeq: %s\r\n\r\n", cseq);
        queue_rtsp_reply(c, resp, rlen);
    }
}

static int process_client_rx(rtsp_client_t *c) {
    int space = RX_BUF_SIZE - 1 - c->rx_len;
    if (space <= 0) { c->rx_len = 0; space = RX_BUF_SIZE - 1; }
    ssize_t n = recv(c->fd, c->rx_buf + c->rx_len, (size_t)space, 0);
    if (n == 0) return -1;
    if (n < 0) return (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : -1;
    c->rx_len += (int)n;
    c->rx_buf[c->rx_len] = '\0';

    for (;;) {
        if (c->rx_len == 0) break;
        if (c->rx_buf[0] == '$') {
            if (c->rx_len < 4) break;
            int plen = 4 + ((c->rx_buf[2] << 8) | c->rx_buf[3]);
            if (plen > RX_BUF_SIZE - 1) { c->rx_len = 0; break; }
            if (c->rx_len < plen) break;
            memmove(c->rx_buf, c->rx_buf + plen, (size_t)(c->rx_len - plen));
            c->rx_len -= plen;
            c->rx_buf[c->rx_len] = '\0';
            continue;
        }
        char *end = strstr((char *)c->rx_buf, "\r\n\r\n");
        if (!end) break;
        int hdr_bytes = (int)(end - (char *)c->rx_buf) + 4;
        int cl = 0;
        *end = '\0';
        const char *clh = find_hdr_ci((char *)c->rx_buf, "Content-Length");
        if (clh) cl = atoi(clh);
        *end = '\r';
        if (cl < 0 || hdr_bytes + cl > RX_BUF_SIZE - 1) return -1;
        if (c->rx_len < hdr_bytes + cl) break;

        char saved = (char)c->rx_buf[hdr_bytes + cl];
        c->rx_buf[hdr_bytes + cl] = '\0';
        handle_rtsp_request(c, (char *)c->rx_buf);
        c->rx_buf[hdr_bytes + cl] = (uint8_t)saved;

        int consumed = hdr_bytes + cl;
        memmove(c->rx_buf, c->rx_buf + consumed, (size_t)(c->rx_len - consumed));
        c->rx_len -= consumed;
        c->rx_buf[c->rx_len] = '\0';
    }
    return 0;
}

static int flush_client_tx(rtsp_client_t *c) {
    pthread_mutex_lock(&c->tx_mu);
    while (c->tx_used > 0) {
        uint32_t chunk = CLIENT_RING_SIZE - c->tx_tail;
        if (chunk > c->tx_used) chunk = c->tx_used;
        ssize_t w = send(c->fd, c->tx_ring + c->tx_tail, chunk, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (w > 0) {
            c->tx_tail = (c->tx_tail + (uint32_t)w) % CLIENT_RING_SIZE;
            c->tx_used -= (uint32_t)w;
        } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            pthread_mutex_unlock(&c->tx_mu);
            return -1;
        }
    }
    pthread_mutex_unlock(&c->tx_mu);
    return 0;
}

static void *rtsp_server_thread(void *arg) {
    (void)arg;
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { rlog("[RTSP] socket() failed errno=%d\n", errno); return NULL; }
    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    set_nonblock(srv);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(RTSP_PORT);

    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(srv, 8) != 0) {
        rlog("[RTSP] bind/listen(%d) failed errno=%d\n", RTSP_PORT, errno);
        close(srv);
        return NULL;
    }
    rlog("[RTSP] listening on 0.0.0.0:%d\n", RTSP_PORT);

    for (;;) {
        struct pollfd pfds[2 + MAX_CLIENTS];
        int map[2 + MAX_CLIENTS];
        int nfds = 0;

        pfds[nfds].fd = srv; pfds[nfds].events = POLLIN; pfds[nfds].revents = 0; map[nfds++] = -1;
        pfds[nfds].fd = g_wakeup_pipe[0]; pfds[nfds].events = POLLIN; pfds[nfds].revents = 0; map[nfds++] = -2;

        int i;
        for (i = 0; i < MAX_CLIENTS; i++) {
            rtsp_client_t *c = &g_clients[i];
            if (!c->in_use || c->fd < 0) continue;
            short ev = POLLIN;
            pthread_mutex_lock(&c->tx_mu);
            if (c->tx_used > 0) ev |= POLLOUT;
            pthread_mutex_unlock(&c->tx_mu);
            pfds[nfds].fd = c->fd;
            pfds[nfds].events = ev;
            pfds[nfds].revents = 0;
            map[nfds++] = i;
        }

        int rc = poll(pfds, (nfds_t)nfds, 1000);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* Drain wakeup pipe */
        if (pfds[1].revents & POLLIN) {
            char drain[64];
            while (read(g_wakeup_pipe[0], drain, sizeof(drain)) > 0) {}
        }

        /* Accept new connections */
        if (pfds[0].revents & POLLIN) {
            for (;;) {
                struct sockaddr_in cli_addr;
                socklen_t alen = sizeof(cli_addr);
                int cfd = accept(srv, (struct sockaddr *)&cli_addr, &alen);
                if (cfd < 0) break;
                set_nonblock(cfd);
                int nd = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));

                int slot = -1;
                pthread_mutex_lock(&g_clients_mu);
                for (i = 0; i < MAX_CLIENTS; i++) {
                    if (!g_clients[i].in_use) { slot = i; break; }
                }
                if (slot >= 0) {
                    rtsp_client_t *c = &g_clients[slot];
                    uint8_t *ring = malloc(CLIENT_RING_SIZE);
                    if (!ring) {
                        slot = -1;
                    } else {
                        c->fd = cfd;
                        c->session_id = g_next_session++;
                        c->peer_addr = cli_addr;
                        struct sockaddr_in local_sa;
                        socklen_t llen = sizeof(local_sa);
                        if (getsockname(cfd, (struct sockaddr *)&local_sa, &llen) == 0) {
                            inet_ntop(AF_INET, &local_sa.sin_addr, c->cam_ip, sizeof(c->cam_ip));
                        } else {
                            strcpy(c->cam_ip, "127.0.0.1");
                        }
                        c->rx_len = 0;
                        c->v_setup = c->v_is_tcp = 0;
                        c->a_setup = c->a_is_tcp = 0;
                        c->v_udp_rtp_fd = c->v_udp_rtcp_fd = -1;
                        c->a_udp_rtp_fd = c->a_udp_rtcp_fd = -1;
                        c->v_seq = 1; c->a_seq = 1;
                        c->v_ssrc = 0x11223344u + (uint32_t)slot;
                        c->a_ssrc = 0x55667788u + (uint32_t)slot;
                        c->playing = 0;
                        c->need_idr = 1;
                        pthread_mutex_lock(&c->tx_mu);
                        c->tx_ring = ring;
                        c->tx_head = c->tx_tail = c->tx_used = 0;
                        pthread_mutex_unlock(&c->tx_mu);
                        c->in_use = 1;
                        rlog("[RTSP] accept slot=%d fd=%d from %s\n", slot, cfd, inet_ntoa(cli_addr.sin_addr));
                    }
                }
                pthread_mutex_unlock(&g_clients_mu);
                if (slot < 0) close(cfd);
                else update_rtsp_demand(1);
            }
        }

        /* Service client read/write */
        for (i = 2; i < nfds; i++) {
            int idx = map[i];
            if (idx < 0) continue;
            rtsp_client_t *c = &g_clients[idx];
            if (!c->in_use) continue;
            short rev = pfds[i].revents;
            if (rev & (POLLERR | POLLHUP | POLLNVAL)) {
                rlog("[RTSP] close slot=%d (hup/err)\n", idx);
                close_client(c);
                continue;
            }
            if (rev & POLLIN) {
                if (process_client_rx(c) != 0) {
                    rlog("[RTSP] close slot=%d (eof/rx)\n", idx);
                    close_client(c);
                    continue;
                }
            }
            if (flush_client_tx(c) != 0) {
                rlog("[RTSP] close slot=%d (tx err)\n", idx);
                close_client(c);
                continue;
            }
        }
    }
    return NULL;
}

/* =========================================================================
 * Symbol resolution & Initialization (runs strictly inside vt_cam only)
 * ========================================================================= */
static pthread_once_t g_init_once = PTHREAD_ONCE_INIT;

static void *resolve_sym(void *lib, const char *name) {
    void *p = dlsym(RTLD_NEXT, name);
    if (!p && lib) p = dlsym(lib, name);
    return p;
}

static void init_rtsp_cam(void) {
    char exe[256] = {0};
    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    int is_vt_cam = (n > 0 && strstr(exe, "vt_cam") != NULL);

    void *lib_iotc = dlopen("/home/root/lib/libIOTCAPIs.so", RTLD_LAZY | RTLD_GLOBAL);
    void *lib_av   = dlopen("/home/root/lib/libAVAPIs.so", RTLD_LAZY | RTLD_GLOBAL);

    real_IOTC_Set_Max_Session_Number = resolve_sym(lib_iotc, "IOTC_Set_Max_Session_Number");
    real_IOTC_Listen                 = resolve_sym(lib_iotc, "IOTC_Listen");
    real_IOTC_Session_Check_Ex       = resolve_sym(lib_iotc, "IOTC_Session_Check_Ex");
    real_IOTC_Session_Close          = resolve_sym(lib_iotc, "IOTC_Session_Close");

    real_avServStartEx               = resolve_sym(lib_av, "avServStartEx");
    real_avServStop                  = resolve_sym(lib_av, "avServStop");
    real_avServSetResendSize         = resolve_sym(lib_av, "avServSetResendSize");
    real_avServResetBuffer           = resolve_sym(lib_av, "avServResetBuffer");
    real_avResendBufUsageRate        = resolve_sym(lib_av, "avResendBufUsageRate");
    real_avRecvIOCtrl                = resolve_sym(lib_av, "avRecvIOCtrl");
    real_avSendIOCtrl                = resolve_sym(lib_av, "avSendIOCtrl");
    real_avSendFrameData             = resolve_sym(lib_av, "avSendFrameData");
    real_avSendAudioData             = resolve_sym(lib_av, "avSendAudioData");
    real_avRecvAudioData             = resolve_sym(lib_av, "avRecvAudioData");

    if (!is_vt_cam) return;
    g_is_vt_cam = 1;

    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        memset(&g_clients[i], 0, sizeof(g_clients[i]));
        g_clients[i].fd = -1;
        g_clients[i].v_udp_rtp_fd = g_clients[i].v_udp_rtcp_fd = -1;
        g_clients[i].a_udp_rtp_fd = g_clients[i].a_udp_rtcp_fd = -1;
        pthread_mutex_init(&g_clients[i].tx_mu, NULL);
    }

    if (pipe(g_wakeup_pipe) == 0) {
        set_nonblock(g_wakeup_pipe[0]);
        set_nonblock(g_wakeup_pipe[1]);
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, 64 * 1024);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_t th;
    pthread_create(&th, &attr, rtsp_server_thread, NULL);
    pthread_attr_destroy(&attr);

    rlog("[init] librtsp_cam started pid=%d exe=%s\n", (int)getpid(), exe);
}

/* =========================================================================
 * Virtual ThroughTek session & real-session state tracking
 * ========================================================================= */
#define MAX_REAL_SESSIONS          16
#define IOCTRL_IPCAM_START         511
#define IOCTRL_IPCAM_STOP          767
#define IOCTRL_IPCAM_AUDIOSTART    768
#define IOCTRL_IPCAM_AUDIOSTOP     769
#define IOCTRL_IPCAM_SET_RES       800
#define IOCTRL_IPCAM_SET_RES_RESP  801
#define IOCTRL_IPCAM_GET_RES       802
#define IOCTRL_IPCAM_GET_RES_RESP  803
#define IOCTRL_IPCAM_DEVINFO_REQ   816
#define IOCTRL_VTECH_IS_LIVE_REQ   1817
#define IOCTRL_VTECH_STREAM_NOTIFY 1871
#define IOCTRL_VTECH_OTHER_LIVE    1882
#define IOCTRL_VTECH_OTHER_LIVE_RP 1883
#define IOCTRL_VTECH_STOP_STREAM   1898

typedef enum {
    VIRT_VID_IDLE = 0,
    VIRT_VID_NEED_START,
    VIRT_VID_RUNNING,
    VIRT_VID_NEED_STOP,
    VIRT_VID_NEED_STOP_CLOSE,
    VIRT_VID_STOPPING,
    VIRT_VID_STOPPED
} virt_vid_state_t;

typedef struct {
    int in_use;
    int sid;
    int av_index;
    int client_type; /* 0=unknown, 1=app, 2=parent_unit */
    int streaming;
} real_session_t;

static real_session_t g_real_sessions[MAX_REAL_SESSIONS];
static pthread_mutex_t g_sess_mu = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t g_virt_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_virt_cv = PTHREAD_COND_INITIALIZER;
static virt_vid_state_t g_virt_vid_state = VIRT_VID_IDLE;
static int g_virt_audio_started = 0;
static int g_virt_audio_need_stop = 0;
static int g_rtsp_want_stream = 0;
static uint64_t g_rtsp_idle_since_ms = 0;
static int g_res_change_active = 0;
static uint64_t g_res_change_deadline_ms = 0;
static uint64_t g_virt_resume_after_ms = 0;

#define IDLE_STOP_GRACE_MS 3000ULL

static volatile int g_virt_injected = 0;
static volatile int g_serv_calls = 0;

static uint64_t mono_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000L);
}

static void abs_realtime_after_ms(struct timespec *out, uint32_t ms) {
    clock_gettime(CLOCK_REALTIME, out);
    out->tv_sec += (time_t)(ms / 1000U);
    out->tv_nsec += (long)((ms % 1000U) * 1000000UL);
    if (out->tv_nsec >= 1000000000L) {
        out->tv_sec += 1;
        out->tv_nsec -= 1000000000L;
    }
}

static void mark_rtsp_clients_need_idr(void) {
    int i;
    pthread_mutex_lock(&g_clients_mu);
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (g_clients[i].in_use) g_clients[i].need_idr = 1;
    }
    pthread_mutex_unlock(&g_clients_mu);
}

static void track_real_session_add(int sid, int av_index) {
    int i, slot = -1;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].av_index == av_index) {
            slot = i;
            break;
        }
        if (!g_real_sessions[i].in_use && slot < 0) slot = i;
    }
    if (slot >= 0) {
        g_real_sessions[slot].in_use = 1;
        g_real_sessions[slot].sid = sid;
        g_real_sessions[slot].av_index = av_index;
        g_real_sessions[slot].client_type = 0;
        g_real_sessions[slot].streaming = 0;
    }
    pthread_mutex_unlock(&g_sess_mu);
}

static void track_real_session_remove_by_av(int av_index) {
    int i;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].av_index == av_index) {
            g_real_sessions[i].in_use = 0;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
}

static void track_real_session_remove_by_sid(int sid) {
    int i;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].sid == sid) {
            g_real_sessions[i].in_use = 0;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
}

static void track_real_session_set_type(int av_index, int client_type) {
    int i;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].av_index == av_index) {
            g_real_sessions[i].client_type = client_type;
            break;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
}

static void track_real_session_set_streaming(int av_index, int streaming) {
    int i;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].av_index == av_index) {
            g_real_sessions[i].streaming = streaming;
            break;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
}

static int has_other_real_streaming(int exclude_av_index) {
    int i, found = 0;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use &&
            g_real_sessions[i].av_index != exclude_av_index &&
            g_real_sessions[i].streaming) {
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
    return found;
}

static int has_real_parent_unit(void) {
    int i, found = 0;
    pthread_mutex_lock(&g_sess_mu);
    for (i = 0; i < MAX_REAL_SESSIONS; i++) {
        if (g_real_sessions[i].in_use && g_real_sessions[i].client_type == 2) {
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_sess_mu);
    return found;
}

/*
 * Keep vt_cam's START(511, 0x78563412) channel-open mode aligned with
 * VIDEO_RESOLUTION when no Parent Unit is connected, preventing 720p override.
 */
static void sync_open_channel_mode(void) {
    static pthread_mutex_t patch_mu = PTHREAD_MUTEX_INITIALIZER;
    if (!g_is_vt_cam) return;

    int pu = has_real_parent_unit();
    int res = *(volatile int *)VIDEO_RES_ADDR;
    int mode = 1;
    if (!pu) {
        if (res == 1080) mode = 0;
        else if (res == 480) mode = 2;
        else if (res == 360) mode = 3;
        else mode = 1;
    }

    pthread_mutex_lock(&patch_mu);
    volatile uint32_t *jal_ins = (volatile uint32_t *)OPEN_CH_JAL_ADDR;
    volatile uint32_t *arg_ins = (volatile uint32_t *)OPEN_CH_ARG_ADDR;
    if (*jal_ins == 0x0c103774u && ((*arg_ins) & 0xffff0000u) == 0x24040000u) {
        uint32_t want = 0x24040000u | ((uint32_t)mode & 0xffffu);
        if (*arg_ins != want) {
            uintptr_t page = (uintptr_t)OPEN_CH_ARG_ADDR & ~(uintptr_t)4095u;
            if (mprotect((void *)page, 4096, PROT_READ | PROT_WRITE | PROT_EXEC) == 0) {
                *arg_ins = want;
                cacheflush((void *)OPEN_CH_ARG_ADDR, 4, ICACHE);
                rlog("[TUTK] synced open-channel mode: res=%d pu=%d -> mode=%d\n", res, pu, mode);
            } else {
                rlog("[TUTK] mprotect failed err=%d\n", errno);
            }
        }
    }
    pthread_mutex_unlock(&patch_mu);
}

/*
 * Suppress STOP_STREAM(1898)'s watchdog exit(0)/reboot check (0x41b0b8: li a0,1 -> 0)
 * while closing VIRTUAL_SID's channel so idle/resolution stops never reboot vt_cam.
 */
static void set_stop_stream_noreboot(int skip) {
    static pthread_mutex_t s_mu = PTHREAD_MUTEX_INITIALIZER;
    if (!g_is_vt_cam) return;
    uint32_t want = skip ? 0x24040000u : 0x24040001u;
    pthread_mutex_lock(&s_mu);
    volatile uint32_t *ins = (volatile uint32_t *)STOP_STREAM_ARG_ADDR;
    if (*ins == 0x24040000u || *ins == 0x24040001u) {
        if (*ins != want) {
            uintptr_t page = (uintptr_t)STOP_STREAM_ARG_ADDR & ~(uintptr_t)4095u;
            if (mprotect((void *)page, 4096, PROT_READ | PROT_WRITE | PROT_EXEC) == 0) {
                *ins = want;
                cacheflush((void *)STOP_STREAM_ARG_ADDR, 4, ICACHE);
            }
        }
    }
    pthread_mutex_unlock(&s_mu);
}

static void update_rtsp_demand(int want) {
    pthread_mutex_lock(&g_virt_mu);
    if (want) {
        g_rtsp_want_stream = 1;
        g_rtsp_idle_since_ms = 0;
        if (!g_res_change_active) {
            if (g_virt_vid_state == VIRT_VID_STOPPED) {
                g_virt_vid_state = VIRT_VID_NEED_START;
                g_virt_resume_after_ms = 0;
                rlog("[TUTK] RTSP client active, starting VIRTUAL_SID stream\n");
            } else if (g_virt_vid_state == VIRT_VID_NEED_STOP) {
                g_virt_vid_state = VIRT_VID_RUNNING;
                g_virt_audio_need_stop = 0;
            }
        }
    } else {
        if (g_rtsp_want_stream) {
            g_rtsp_want_stream = 0;
            g_rtsp_idle_since_ms = mono_ms();
        }
    }
    pthread_cond_broadcast(&g_virt_cv);
    pthread_mutex_unlock(&g_virt_mu);
}

static void pause_virtual_video_for_res_change(void) {
    pthread_mutex_lock(&g_virt_mu);
    g_res_change_active = 1;
    g_res_change_deadline_ms = mono_ms() + 6000ULL;

    if (g_virt_vid_state == VIRT_VID_NEED_START) {
        g_virt_vid_state = VIRT_VID_STOPPED;
        pthread_cond_broadcast(&g_virt_cv);
    } else if (g_virt_vid_state == VIRT_VID_RUNNING ||
               g_virt_vid_state == VIRT_VID_NEED_STOP ||
               g_virt_vid_state == VIRT_VID_NEED_STOP_CLOSE ||
               g_virt_vid_state == VIRT_VID_STOPPING) {
        if (g_virt_vid_state == VIRT_VID_RUNNING) {
            g_virt_vid_state = VIRT_VID_NEED_STOP;
        }
        pthread_cond_broadcast(&g_virt_cv);
        struct timespec dl;
        abs_realtime_after_ms(&dl, 5000);
        while (g_virt_vid_state != VIRT_VID_STOPPED &&
               g_virt_vid_state != VIRT_VID_IDLE) {
            if (pthread_cond_timedwait(&g_virt_cv, &g_virt_mu, &dl) == ETIMEDOUT) {
                rlog("[TUTK] timed out waiting for VIRTUAL_SID video stop (state=%d)\n",
                     (int)g_virt_vid_state);
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_virt_mu);
}

static void resume_virtual_video_after_res_change(void) {
    pthread_mutex_lock(&g_ps_mu);
    g_sps_len = 0;
    g_pps_len = 0;
    pthread_mutex_unlock(&g_ps_mu);

    pthread_mutex_lock(&g_virt_mu);
    g_res_change_active = 0;
    if (g_virt_vid_state == VIRT_VID_STOPPED && g_rtsp_want_stream) {
        g_virt_vid_state = VIRT_VID_NEED_START;
        g_virt_resume_after_ms = mono_ms() + 150ULL;
        pthread_cond_broadcast(&g_virt_cv);
    }
    pthread_mutex_unlock(&g_virt_mu);
    mark_rtsp_clients_need_idr();
}

int IOTC_Set_Max_Session_Number(unsigned int num) {
    pthread_once(&g_init_once, init_rtsp_cam);
    unsigned int capped = (num > 1) ? (num - 1) : num;
    return real_IOTC_Set_Max_Session_Number ? real_IOTC_Set_Max_Session_Number(capped) : 0;
}

int IOTC_Listen(unsigned int timeout_ms) {
    pthread_once(&g_init_once, init_rtsp_cam);
    g_serv_calls++;
    if (!g_virt_injected && g_serv_calls >= 2) {
        g_virt_injected = 1;
        rlog("[TUTK] injecting VIRTUAL_SID=%d\n", VIRTUAL_SID);
        return VIRTUAL_SID;
    }
    if (!real_IOTC_Listen) {
        usleep(timeout_ms * 1000);
        return -13;
    }
    return real_IOTC_Listen(timeout_ms);
}

int IOTC_Session_Check_Ex(int sid, void *s_info) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (sid == VIRTUAL_SID) {
        uint8_t *p = (uint8_t *)s_info;
        uint32_t sz = *(uint32_t *)p;
        if (sz == 0 || sz > 256) sz = 156;
        memset(p, 0, sz);
        *(uint32_t *)p = sz;
        p[4] = 2;
        p[94] = 1;
        if (sz >= 120) strcpy((char *)(p + 104), "127.0.0.1");
        if (sz >= 156) *(uint16_t *)(p + 152) = 554;
        return 0;
    }
    return real_IOTC_Session_Check_Ex ? real_IOTC_Session_Check_Ex(sid, s_info) : -1;
}

void IOTC_Session_Close(int sid) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (sid == VIRTUAL_SID) {
        g_virt_injected = 0;
        pthread_mutex_lock(&g_virt_mu);
        g_virt_vid_state = VIRT_VID_IDLE;
        g_virt_audio_started = 0;
        g_res_change_active = 0;
        pthread_cond_broadcast(&g_virt_cv);
        pthread_mutex_unlock(&g_virt_mu);
        rlog("[TUTK] IOTC_Session_Close VIRTUAL_SID\n");
        return;
    }
    track_real_session_remove_by_sid(sid);
    rlog("[TUTK] IOTC_Session_Close sid=%d\n", sid);
    if (real_IOTC_Session_Close) real_IOTC_Session_Close(sid);
}

int avServStartEx(const void *in, void *out) {
    pthread_once(&g_init_once, init_rtsp_cam);
    const int *in_i = (const int *)in;
    int sid = in_i ? in_i[1] : -1;
    if (sid == VIRTUAL_SID) {
        pthread_mutex_lock(&g_virt_mu);
        g_virt_vid_state = g_rtsp_want_stream ? VIRT_VID_NEED_START : VIRT_VID_STOPPED;
        g_virt_audio_started = 0;
        g_virt_audio_need_stop = 0;
        g_res_change_active = 0;
        g_virt_resume_after_ms = 0;
        pthread_cond_broadcast(&g_virt_cv);
        pthread_mutex_unlock(&g_virt_mu);
        rlog("[TUTK] avServStartEx VIRTUAL_SID -> avIndex=%d (%s)\n",
             VIRTUAL_AVINDEX, g_rtsp_want_stream ? "ACTIVE" : "IDLE");
        return VIRTUAL_AVINDEX;
    }
    int ret = real_avServStartEx ? real_avServStartEx(in, out) : -1;
    if (ret >= 0) {
        track_real_session_add(sid, ret);
        rlog("[TUTK] real avServStartEx sid=%d -> avIndex=%d\n", sid, ret);
    }
    return ret;
}

void avServStop(int avIndex) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) {
        pthread_mutex_lock(&g_virt_mu);
        g_virt_vid_state = VIRT_VID_IDLE;
        g_virt_audio_started = 0;
        g_res_change_active = 0;
        pthread_cond_broadcast(&g_virt_cv);
        pthread_mutex_unlock(&g_virt_mu);
        rlog("[TUTK] avServStop VIRTUAL_AVINDEX\n");
        return;
    }
    track_real_session_remove_by_av(avIndex);
    rlog("[TUTK] real avServStop avIndex=%d\n", avIndex);
    if (real_avServStop) real_avServStop(avIndex);
}

int avServSetResendSize(int avIndex, unsigned int size) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) return 0;
    return real_avServSetResendSize ? real_avServSetResendSize(avIndex, size) : 0;
}

int avServResetBuffer(int avIndex, int target, unsigned int timeout_ms) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) return 0;
    return real_avServResetBuffer ? real_avServResetBuffer(avIndex, target, timeout_ms) : 0;
}

float avResendBufUsageRate(int avIndex) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) return 0.0f;
    return real_avResendBufUsageRate ? real_avResendBufUsageRate(avIndex) : 0.0f;
}

static int emit_virtual_ioctrl(unsigned int *pioType, char *buf, int maxLen, unsigned int io, int set_magic) {
    int n = (maxLen >= 8) ? 8 : maxLen;
    if (pioType) *pioType = io;
    if (buf && maxLen > 0) {
        memset(buf, 0, (size_t)(maxLen >= 16 ? 16 : maxLen));
        if (set_magic && maxLen >= 8) {
            uint32_t zero = 0;
            uint32_t magic = 0x78563412u;
            memcpy(buf + 0, &zero, 4);
            memcpy(buf + 4, &magic, 4);
        }
    }
    return n;
}

int avRecvIOCtrl(int avIndex, unsigned int *pioType, char *buf, int maxLen, unsigned int timeout_ms) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) {
        /* Clear in-memory E2E privacy gate so 0x78563412 plaintext feed remains active */
        volatile uint8_t *priv = (volatile uint8_t *)PRIVACY_FLAG_ADDR;
        if (*priv != 0) *priv = 0;

        uint32_t slice_ms = (timeout_ms > 200U) ? 200U : timeout_ms;
        uint64_t end_ms = mono_ms() + (uint64_t)slice_ms;

        pthread_mutex_lock(&g_virt_mu);
        if (g_virt_vid_state == VIRT_VID_STOPPING) {
            if (g_rtsp_want_stream && !g_res_change_active) {
                g_virt_vid_state = VIRT_VID_NEED_START;
                g_virt_resume_after_ms = 0;
            } else {
                g_virt_vid_state = VIRT_VID_STOPPED;
            }
            pthread_cond_broadcast(&g_virt_cv);
            pthread_mutex_unlock(&g_virt_mu);
            set_stop_stream_noreboot(0);
            rlog("[TUTK] VIRTUAL_SID video stopped\n");
            pthread_mutex_lock(&g_virt_mu);
        }

        for (;;) {
            uint64_t now = mono_ms();
            if (g_res_change_active && now >= g_res_change_deadline_ms) {
                g_res_change_active = 0;
                if (g_virt_vid_state == VIRT_VID_STOPPED && g_rtsp_want_stream) {
                    g_virt_vid_state = VIRT_VID_NEED_START;
                    g_virt_resume_after_ms = 0;
                    rlog("[TUTK] res change deadline expired, resuming VIRTUAL_SID video\n");
                }
            }

            if (!g_rtsp_want_stream && !g_res_change_active &&
                g_virt_vid_state == VIRT_VID_RUNNING &&
                (now - g_rtsp_idle_since_ms) >= IDLE_STOP_GRACE_MS) {
                g_virt_vid_state = VIRT_VID_NEED_STOP;
                if (g_virt_audio_started) g_virt_audio_need_stop = 1;
                rlog("[TUTK] RTSP idle for %ums, stopping VIRTUAL_SID stream\n", (unsigned)IDLE_STOP_GRACE_MS);
            }

            if (g_virt_audio_need_stop) {
                g_virt_audio_need_stop = 0;
                g_virt_audio_started = 0;
                pthread_mutex_unlock(&g_virt_mu);
                rlog("[TUTK] VIRTUAL_SID emitting AUDIOSTOP(769)\n");
                return emit_virtual_ioctrl(pioType, buf, maxLen, IOCTRL_IPCAM_AUDIOSTOP, 0);
            }

            if (g_virt_vid_state == VIRT_VID_NEED_STOP) {
                g_virt_vid_state = VIRT_VID_NEED_STOP_CLOSE;
                pthread_mutex_unlock(&g_virt_mu);
                rlog("[TUTK] VIRTUAL_SID emitting STOP(767)\n");
                return emit_virtual_ioctrl(pioType, buf, maxLen, IOCTRL_IPCAM_STOP, 0);
            }

            if (g_virt_vid_state == VIRT_VID_NEED_STOP_CLOSE) {
                g_virt_vid_state = VIRT_VID_STOPPING;
                pthread_mutex_unlock(&g_virt_mu);
                set_stop_stream_noreboot(1);
                pthread_mutex_lock(&g_ps_mu);
                g_sps_len = 0;
                g_pps_len = 0;
                pthread_mutex_unlock(&g_ps_mu);
                rlog("[TUTK] VIRTUAL_SID emitting STOP_STREAM(1898)\n");
                return emit_virtual_ioctrl(pioType, buf, maxLen, IOCTRL_VTECH_STOP_STREAM, 0);
            }

            if (g_virt_vid_state == VIRT_VID_NEED_START && now >= g_virt_resume_after_ms) {
                if (g_res_change_active || !g_rtsp_want_stream) {
                    g_virt_vid_state = VIRT_VID_STOPPED;
                    continue;
                }
                g_virt_vid_state = VIRT_VID_RUNNING;
                pthread_mutex_unlock(&g_virt_mu);
                sync_open_channel_mode();
                rlog("[TUTK] VIRTUAL_SID emitting START(511)\n");
                return emit_virtual_ioctrl(pioType, buf, maxLen, IOCTRL_IPCAM_START, 1);
            }

            if (!g_virt_audio_started && g_virt_vid_state == VIRT_VID_RUNNING && g_rtsp_want_stream) {
                g_virt_audio_started = 1;
                pthread_mutex_unlock(&g_virt_mu);
                return emit_virtual_ioctrl(pioType, buf, maxLen, IOCTRL_IPCAM_AUDIOSTART, 0);
            }

            if (now >= end_ms) {
                pthread_mutex_unlock(&g_virt_mu);
                return -20011; /* AV_ER_TIMEOUT */
            }

            uint64_t wait_until = end_ms;
            if (g_virt_vid_state == VIRT_VID_NEED_START && g_virt_resume_after_ms > now &&
                g_virt_resume_after_ms < wait_until) {
                wait_until = g_virt_resume_after_ms;
            }
            if (!g_rtsp_want_stream && !g_res_change_active && g_virt_vid_state == VIRT_VID_RUNNING) {
                uint64_t idle_dl = g_rtsp_idle_since_ms + IDLE_STOP_GRACE_MS;
                if (idle_dl > now && idle_dl < wait_until) wait_until = idle_dl;
            }
            if (g_res_change_active && g_res_change_deadline_ms > now &&
                g_res_change_deadline_ms < wait_until) {
                wait_until = g_res_change_deadline_ms;
            }
            struct timespec ts;
            abs_realtime_after_ms(&ts, (uint32_t)(wait_until - now));
            pthread_cond_timedwait(&g_virt_cv, &g_virt_mu, &ts);
        }
    }

    int r = real_avRecvIOCtrl ? real_avRecvIOCtrl(avIndex, pioType, buf, maxLen, timeout_ms) : -20011;
    if (r >= 0 && pioType) {
        unsigned int io = *pioType;
        if (io == IOCTRL_IPCAM_DEVINFO_REQ) {
            int ctype = (buf && r >= 2 && (uint8_t)buf[0] == 0 && (uint8_t)buf[1] == 1) ? 1 : 2;
            track_real_session_set_type(avIndex, ctype);
            sync_open_channel_mode();
            rlog("[TUTK] real avIndex=%d rx DEVINFO_REQ(816) client_type=%d\n", avIndex, ctype);
        } else if (io == IOCTRL_IPCAM_START) {
            uint32_t magic = 0;
            if (buf && r >= 8) memcpy(&magic, buf + 4, 4);
            track_real_session_set_streaming(avIndex, 1);
            sync_open_channel_mode();
            rlog("[TUTK] real avIndex=%d rx START(511) len=%d magic=0x%08x\n", avIndex, r, magic);
        } else if (io == IOCTRL_IPCAM_STOP) {
            track_real_session_set_streaming(avIndex, 0);
            rlog("[TUTK] real avIndex=%d rx STOP(767)\n", avIndex);
        } else if (io == IOCTRL_VTECH_STOP_STREAM) {
            track_real_session_set_streaming(avIndex, 0);
            rlog("[TUTK] real avIndex=%d rx STOP_STREAM(1898)\n", avIndex);
        } else if (io == IOCTRL_IPCAM_SET_RES) {
            uint8_t val = (buf && r >= 5) ? (uint8_t)buf[4] : 255;
            rlog("[TUTK] real avIndex=%d rx SET_RES(800) val=%u\n", avIndex, val);
            pause_virtual_video_for_res_change();
        } else if (io == IOCTRL_IPCAM_GET_RES || io == IOCTRL_VTECH_OTHER_LIVE ||
                   io == IOCTRL_VTECH_IS_LIVE_REQ) {
            rlog("[TUTK] real avIndex=%d rx ioType=%u len=%d\n", avIndex, io, r);
        }
    }
    return r;
}

int avSendIOCtrl(int avIndex, unsigned int ioType, const char *buf, int len) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) return 0;

    char masked[64];
    const char *send_buf = buf;

    if ((ioType == IOCTRL_VTECH_STREAM_NOTIFY || ioType == IOCTRL_VTECH_OTHER_LIVE_RP) &&
        buf && len >= 4 && len <= (int)sizeof(masked)) {
        int val = 0;
        memcpy(&val, buf, 4);
        if (val != 0 && !has_other_real_streaming(avIndex)) {
            memcpy(masked, buf, (size_t)len);
            memset(masked, 0, 4);
            send_buf = masked;
            rlog("[TUTK] real avIndex=%d tx ioType=%u masked %d->0\n", avIndex, ioType, val);
        } else {
            rlog("[TUTK] real avIndex=%d tx ioType=%u val=%d\n", avIndex, ioType, val);
        }
    } else if (ioType == IOCTRL_IPCAM_SET_RES_RESP || ioType == IOCTRL_IPCAM_GET_RES_RESP) {
        int v0 = (buf && len >= 4) ? ((const int *)buf)[0] : -999;
        int v1 = (buf && len >= 8) ? ((const int *)buf)[1] : 0;
        rlog("[TUTK] real avIndex=%d tx ioType=%u len=%d v0=%d v1=%d\n",
             avIndex, ioType, len, v0, v1);
    }

    int r = real_avSendIOCtrl ? real_avSendIOCtrl(avIndex, ioType, send_buf, len) : 0;

    if (ioType == IOCTRL_IPCAM_SET_RES_RESP) {
        resume_virtual_video_after_res_change();
    }
    return r;
}

int avSendFrameData(int avIndex, const char *data, int size, const void *info, int infoSize) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) {
        if (data && size > 0) {
            const uint8_t *fi = (const uint8_t *)info;
            uint8_t flags = (fi && infoSize >= 3) ? fi[2] : 0;
            uint32_t ts = (fi && infoSize >= 16) ? *(const uint32_t *)(fi + 12) : 0;
            fanout_video_frame((const uint8_t *)data, size, flags, ts);
        }
        return 0;
    }
    return real_avSendFrameData ? real_avSendFrameData(avIndex, data, size, info, infoSize) : 0;
}

int avSendAudioData(int avIndex, const char *data, int size, const void *info, int infoSize) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) {
        if (data && size > 0) {
            const uint8_t *fi = (const uint8_t *)info;
            uint32_t ts = (fi && infoSize >= 16) ? *(const uint32_t *)(fi + 12) : 0;
            fanout_audio_frame((const uint8_t *)data, size, ts);
        }
        return 0;
    }
    return real_avSendAudioData ? real_avSendAudioData(avIndex, data, size, info, infoSize) : 0;
}

int avRecvAudioData(int avIndex, char *buf, int maxLen, void *info, unsigned int *pFrameIdx) {
    pthread_once(&g_init_once, init_rtsp_cam);
    if (avIndex == VIRTUAL_AVINDEX) {
        usleep(40000);
        return -20012;
    }
    return real_avRecvAudioData ? real_avRecvAudioData(avIndex, buf, maxLen, info, pFrameIdx) : -20012;
}
