import base64, hashlib, os, sys
from tel import connect_telnet, run_cmd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SRC = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "bin", "librtsp_cam.so"))

src = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SRC
dst = sys.argv[2] if len(sys.argv) > 2 else "/usr/lib/librtsp_cam.so"
mode = sys.argv[3] if len(sys.argv) > 3 else "755"
dst_new = dst + ".new"

with open(src, "rb") as f:
    raw = f.read()

expected_md5 = hashlib.md5(raw).hexdigest()
b64 = base64.b64encode(raw).decode("ascii")
print(f"Local {src} -> {dst}: size={len(raw)}, md5={expected_md5}, b64_len={len(b64)}")

s, _ = connect_telnet()
print(run_cmd(s, f"rm -f /tmp/up.b64 {dst_new}"))

lines = [b64[i : i + 76] for i in range(0, len(b64), 76)]
chunk_size = 20
for idx in range(0, len(lines), chunk_size):
    part = "\n".join(lines[idx : idx + chunk_size])
    cmd = f"cat << 'EOF' >> /tmp/up.b64\n{part}\nEOF"
    run_cmd(s, cmd, timeout=10)
    if (idx // chunk_size) % 10 == 0:
        print(f"Sent {min(idx + chunk_size, len(lines))}/{len(lines)} lines")

res = run_cmd(
    s,
    f"base64 -d /tmp/up.b64 > {dst_new} && chmod {mode} {dst_new} && md5sum {dst_new} && ls -la {dst_new}",
    timeout=15,
)
print(res)
if expected_md5 not in res:
    print(f"ERROR: MD5 mismatch! Not overwriting {dst}")
    s.close()
    sys.exit(1)

res2 = run_cmd(s, f"mv {dst_new} {dst} && rm -f /tmp/up.b64 && sync && md5sum {dst}", timeout=15)
print(res2)
s.close()
