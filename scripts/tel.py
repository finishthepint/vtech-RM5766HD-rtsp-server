import os, socket, sys, time

DEFAULT_HOST = os.environ.get("CAM_HOST", "192.168.1.243")


def connect_telnet(host=None, port=23, timeout=10):
    if host is None:
        host = DEFAULT_HOST
    s = socket.create_connection((host, port), timeout=timeout)
    buf = b""
    end = time.time() + timeout
    logged_in = False
    while time.time() < end:
        try:
            s.settimeout(0.5)
            data = s.recv(4096)
            if not data:
                break
            clean = bytearray()
            i = 0
            while i < len(data):
                if data[i] == 255 and i + 2 < len(data):
                    cmd, opt = data[i + 1], data[i + 2]
                    if cmd == 253:
                        s.sendall(bytes([255, 252, opt]))
                    elif cmd == 251:
                        s.sendall(bytes([255, 254, opt]))
                    i += 3
                else:
                    clean.append(data[i])
                    i += 1
            buf += bytes(clean)
            if not logged_in and (b"login:" in buf or b"Login:" in buf):
                s.sendall(b"root\n")
                logged_in = True
                buf = b""
            elif b"Password:" in buf:
                s.sendall(b"\n")
                buf = b""
            elif b"# " in buf or buf.endswith(b"#") or b"~ #" in buf:
                return s, buf
        except socket.timeout:
            if not logged_in and buf:
                s.sendall(b"\n")
            elif logged_in:
                return s, buf
    raise RuntimeError(f"Telnet login timeout, buf={buf!r}")


def run_cmd(s, cmd, timeout=15):
    marker = "__END_CMD_MARKER__"
    full = f"{cmd}\necho {marker}\n".encode()
    s.sendall(full)
    out = b""
    end = time.time() + timeout
    while time.time() < end:
        try:
            s.settimeout(0.5)
            chunk = s.recv(8192)
            if not chunk:
                break
            out += chunk
            text = out.decode("utf-8", "replace")
            if f"\n{marker}" in text or f"\r\n{marker}" in text:
                return text
        except socket.timeout:
            pass
    return out.decode("utf-8", "replace")


if __name__ == "__main__":
    s, init = connect_telnet()
    cmd = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "id"
    print(run_cmd(s, cmd))
    s.close()
