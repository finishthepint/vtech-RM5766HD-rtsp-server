import base64, os, re, socket, sys, time

def remove_emulation_prevention(data):
    out = bytearray()
    i = 0
    while i < len(data):
        if i + 2 < len(data) and data[i] == 0 and data[i+1] == 0 and data[i+2] == 3:
            out.extend((0, 0))
            i += 3
        else:
            out.append(data[i])
            i += 1
    return bytes(out)

class BitReader:
    def __init__(self, data):
        self.data = data
        self.bitpos = 0
    def read_bit(self):
        b = (self.data[self.bitpos >> 3] >> (7 - (self.bitpos & 7))) & 1
        self.bitpos += 1
        return b
    def read_bits(self, n):
        v = 0
        for _ in range(n):
            v = (v << 1) | self.read_bit()
        return v
    def read_ue(self):
        zeros = 0
        while self.read_bit() == 0:
            zeros += 1
        if zeros == 0:
            return 0
        return ((1 << zeros) - 1) + self.read_bits(zeros)
    def read_se(self):
        v = self.read_ue()
        return (v + 1) // 2 if (v & 1) else -(v // 2)

def parse_sps_resolution(sps_nal):
    rbsp = remove_emulation_prevention(sps_nal[1:])
    br = BitReader(rbsp)
    profile_idc = br.read_bits(8)
    constraint_flags = br.read_bits(8)
    level_idc = br.read_bits(8)
    seq_param_set_id = br.read_ue()
    if profile_idc in (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134):
        chroma_format_idc = br.read_ue()
        if chroma_format_idc == 3:
            br.read_bit()
        br.read_ue()
        br.read_ue()
        br.read_bit()
        seq_scaling_matrix_present_flag = br.read_bit()
        if seq_scaling_matrix_present_flag:
            cnt = 8 if chroma_format_idc != 3 else 12
            for i in range(cnt):
                if br.read_bit():
                    size = 16 if i < 6 else 64
                    last_scale, next_scale = 8, 8
                    for _ in range(size):
                        if next_scale != 0:
                            delta_scale = br.read_se()
                            next_scale = (last_scale + delta_scale + 256) % 256
                        last_scale = next_scale if next_scale != 0 else last_scale
    log2_max_frame_num_minus4 = br.read_ue()
    pic_order_cnt_type = br.read_ue()
    if pic_order_cnt_type == 0:
        br.read_ue()
    elif pic_order_cnt_type == 1:
        br.read_bit()
        br.read_se()
        br.read_se()
        num_ref_frames_in_pic_order_cnt_cycle = br.read_ue()
        for _ in range(num_ref_frames_in_pic_order_cnt_cycle):
            br.read_se()
    br.read_ue() # max_num_ref_frames
    br.read_bit() # gaps_in_frame_num_value_allowed_flag
    pic_width_in_mbs_minus1 = br.read_ue()
    pic_height_in_map_units_minus1 = br.read_ue()
    frame_mbs_only_flag = br.read_bit()
    if not frame_mbs_only_flag:
        br.read_bit()
    br.read_bit() # direct_8x8_inference_flag
    frame_cropping_flag = br.read_bit()
    crop_left = crop_right = crop_top = crop_bottom = 0
    if frame_cropping_flag:
        crop_left = br.read_ue()
        crop_right = br.read_ue()
        crop_top = br.read_ue()
        crop_bottom = br.read_ue()
    width = (pic_width_in_mbs_minus1 + 1) * 16 - (crop_left + crop_right) * 2
    height = (2 - frame_mbs_only_flag) * (pic_height_in_map_units_minus1 + 1) * 16 - (crop_top + crop_bottom) * 2 * (2 - frame_mbs_only_flag)
    return profile_idc, level_idc, width, height

def recv_rtsp_msg(s):
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    hdr, rest = buf.split(b"\r\n\r\n", 1)
    hdr_text = hdr.decode("latin1")
    m = re.search(r"Content-Length:\s*(\d+)", hdr_text, re.I)
    cl = int(m.group(1)) if m else 0
    while len(rest) < cl:
        chunk = s.recv(4096)
        if not chunk:
            break
        rest += chunk
    return hdr_text, rest[:cl].decode("latin1", "replace"), rest[cl:]

def probe(host=None, port=554):
    if host is None:
        host = os.environ.get("CAM_HOST", "192.168.1.243")
    t_conn = time.time()
    s = socket.create_connection((host, port), timeout=10)
    s.settimeout(10)
    url = f"rtsp://{host}:{port}/live"
    s.sendall(f"DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n".encode())
    hdr, sdp, _ = recv_rtsp_msg(s)
    t_desc = time.time() - t_conn
    print(f"=== DESCRIBE ({t_desc:.2f}s) ===")
    print(hdr)
    if "200 OK" not in hdr.splitlines()[0]:
        s.close()
        raise RuntimeError(f"DESCRIBE failed: {hdr.splitlines()[0]}")
    print(sdp.strip())
    m = re.search(r"sprop-parameter-sets=([^;\r\n]+)", sdp)
    if m:
        parts = m.group(1).split(",")
        sps = base64.b64decode(parts[0])
        prof, lvl, w, h = parse_sps_resolution(sps)
        print(f"SDP SPS: profile={prof}, level={lvl}, resolution={w}x{h}")
    # Also SETUP + PLAY briefly to read interleaved packets
    m_ctrl = re.search(r"a=control:(track\d+|video[^\r\n]*|[^\r\n]*trackID[^\r\n]*)", sdp)
    track = m_ctrl.group(1) if m_ctrl else "track0"
    setup_url = track if track.startswith("rtsp://") else f"{url}/{track}"
    s.sendall(f"SETUP {setup_url} RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n".encode())
    shdr, _, _ = recv_rtsp_msg(s)
    msess = re.search(r"Session:\s*([^;\r\n]+)", shdr, re.I)
    sess = msess.group(1).strip() if msess else "1"
    s.sendall(f"PLAY {url} RTSP/1.0\r\nCSeq: 3\r\nSession: {sess}\r\nRange: npt=0.000-\r\n\r\n".encode())
    phdr, _, extra = recv_rtsp_msg(s)
    print("=== PLAY ===")
    print(phdr.splitlines()[0])
    buf = extra
    t0 = time.time()
    rtp_pkts = 0
    while time.time() - t0 < 5.0 and rtp_pkts < 20:
        try:
            s.settimeout(4.0)
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
            while len(buf) >= 4 and buf[0] == 0x24:
                plen = (buf[2] << 8) | buf[3]
                if len(buf) < 4 + plen:
                    break
                pkt = buf[4:4+plen]
                buf = buf[4+plen:]
                if buf and buf[0] != 0x24:
                    pass
                if buf == b"" or len(pkt) > 12:
                    ch = 0
                    rtp_pkts += 1
        except socket.timeout:
            break
    print(f"Received {rtp_pkts} interleaved packets in {time.time()-t0:.2f}s")
    s.sendall(f"TEARDOWN {url} RTSP/1.0\r\nCSeq: 4\r\nSession: {sess}\r\n\r\n".encode())
    s.close()
    if rtp_pkts == 0:
        raise RuntimeError("No RTP packets received")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--cycle":
        print("--- Pass 1 (cold start) ---")
        probe()
        print("--- Waiting 6s for idle auto-stop ---")
        time.sleep(6.0)
        print("--- Pass 2 (wake from idle stop) ---")
        probe()
    else:
        probe()
