#!/usr/bin/env python3
"""
Test script for pubsubclient - tests standalone argument/validation behaviour.
Run with: python3 test_pubsubclient.py
Assumes pubsubclient.py is in the same directory.
"""

import subprocess
import sys
import socket
import threading
import time

CLIENT = ["python3", "pubsubclient.py"]
PASS = 0
FAIL = 0

def run(args, stdin_input=None, timeout=3):
    """Run pubsubclient with given args, return (stdout, stderr, returncode)."""
    result = subprocess.run(
        CLIENT + args,
        input=stdin_input,
        capture_output=True,
        text=True,
        timeout=timeout
    )
    return result.stdout, result.stderr, result.returncode

def check(test_name, stdout, stderr, returncode,
          expect_stdout=None, expect_stderr=None, expect_exit=None):
    """Check results and print pass/fail."""
    global PASS, FAIL
    ok = True

    if expect_exit is not None and returncode != expect_exit:
        print(f"FAIL [{test_name}] exit={returncode}, expected {expect_exit}")
        ok = False
    if expect_stderr is not None and expect_stderr not in stderr:
        print(f"FAIL [{test_name}] stderr={repr(stderr)}, expected {repr(expect_stderr)}")
        ok = False
    if expect_stdout is not None and expect_stdout not in stdout:
        print(f"FAIL [{test_name}] stdout={repr(stdout)}, expected {repr(expect_stdout)}")
        ok = False

    if ok:
        print(f"PASS [{test_name}]")
        PASS += 1
    else:
        FAIL += 1

USAGE_MSG = "Usage: pubsubclient [--topic topic] [server]:port clientid [message]"

# ─────────────────────────────────────────────
# 1. USAGE / ARGUMENT ERRORS (exit 1)
# ─────────────────────────────────────────────
print("\n=== Usage Errors (exit 1) ===")

_, err, code = run([])
check("no args", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic"])
check("--topic with no value", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run([":3200"])
check("missing clientid", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["localhost:3200"])
check("missing clientid (just server:port)", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic", "heat", "--topic", "cold", ":3200", "c1"])
check("duplicate --topic", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run([":3200", "c1", "hello"])
check("message without --topic", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic", "heat", ":3200", "c1", "hello", "extra"])
check("too many args", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic", "heat", ":", "c1"])
check("empty port", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic", "heat", "", "c1"])
check("empty server:port", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

_, err, code = run(["--topic", "heat", "3200", "c1"])
check("no colon in server:port", "", err, code, expect_stderr=USAGE_MSG, expect_exit=1)

# ─────────────────────────────────────────────
# 2. CLIENT ID ERRORS (exit 4)
# ─────────────────────────────────────────────
print("\n=== Client ID Errors (exit 4) ===")

_, err, code = run([":3200", "a"])
check("clientid too short (1 char)", "", err, code,
      expect_stderr='pubsubclient: bad client ID "a"', expect_exit=4)

_, err, code = run([":3200", "a" * 33])
check("clientid too long (33 chars)", "", err, code,
      expect_stderr=f'pubsubclient: bad client ID "{"a"*33}"', expect_exit=4)

_, err, code = run([":3200", "ab!"])
check("clientid with invalid char !", "", err, code,
      expect_stderr='pubsubclient: bad client ID "ab!"', expect_exit=4)

_, err, code = run([":3200", "ab cd"])
check("clientid with space", "", err, code,
      expect_stderr='pubsubclient: bad client ID "ab cd"', expect_exit=4)

_, err, code = run([":3200", "ab_cd"])
check("clientid with underscore", "", err, code,
      expect_stderr='pubsubclient: bad client ID "ab_cd"', expect_exit=4)

# ─────────────────────────────────────────────
# 3. TOPIC ERRORS (exit 5)
# ─────────────────────────────────────────────
print("\n=== Topic Errors (exit 5) ===")

_, err, code = run(["--topic", "123bad", ":3200", "c1"])
check("topic starts with digit", "", err, code,
      expect_stderr='pubsubclient: invalid topic string "123bad"', expect_exit=5)

_, err, code = run(["--topic", "/badstart", ":3200", "c1"])
check("topic starts with /", "", err, code,
      expect_stderr='pubsubclient: invalid topic string "/badstart"', expect_exit=5)

_, err, code = run(["--topic", "bad@topic", ":3200", "c1"])
check("topic with @ char", "", err, code,
      expect_stderr='pubsubclient: invalid topic string "bad@topic"', expect_exit=5)

_, err, code = run(["--topic", "good/topic", ":3200", "c1"])
# This should NOT exit 5 - valid topic, will fail at connection (exit 7)
check("valid topic passes topic check", "", err, code,
      expect_exit=7)

# ─────────────────────────────────────────────
# 4. MESSAGE ERRORS (exit 6)
# ─────────────────────────────────────────────
print("\n=== Message Errors (exit 6) ===")

_, err, code = run(["--topic", "heat", ":3200", "c1", "hello\x01world"])
check("message with non-printable char", "", err, code,
      expect_stderr="pubsubclient: messages must only contain printable characters",
      expect_exit=6)

_, err, code = run(["--topic", "heat", ":3200", "c1", "hello\tworld"])
check("message with tab (non-printable)", "", err, code,
      expect_stderr="pubsubclient: messages must only contain printable characters",
      expect_exit=6)

# ─────────────────────────────────────────────
# 5. CONNECTION ERRORS (exit 7)
# ─────────────────────────────────────────────
print("\n=== Connection Errors (exit 7) ===")

_, err, code = run([":19999", "c1"])
check("nothing listening on port", "", err, code,
      expect_stderr='pubsubclient: unable to connect to "localhost:19999"',
      expect_exit=7)

_, err, code = run(["999.999.999.999:3200", "c1"])
check("invalid IP address", "", err, code,
      expect_stderr='pubsubclient: unable to connect to "999.999.999.999:3200"',
      expect_exit=7)

_, err, code = run(["notahost:3200", "c1"])
check("invalid hostname", "", err, code,
      expect_stderr='pubsubclient: unable to connect to "notahost:3200"',
      expect_exit=7)

# ─────────────────────────────────────────────
# 6. SERVER VALIDITY (exit 8) - connect to a non-pubsubserver
# ─────────────────────────────────────────────
print("\n=== Server Validity (exit 8) ===")

def fake_server(port, behavior="silent"):
    """Start a fake TCP server that does nothing useful."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("", port))
    s.listen(1)
    s.settimeout(5)
    try:
        conn, _ = s.accept()
        if behavior == "garbage":
            conn.sendall(b"hello i am not a pubsubserver\n")
        # "silent" just does nothing
        time.sleep(2)
        conn.close()
    except Exception:
        pass
    finally:
        s.close()

# Test: server sends garbage
t = threading.Thread(target=fake_server, args=(19998, "garbage"), daemon=True)
t.start()
time.sleep(0.2)
_, err, code = run([":19998", "c1"], timeout=4)
check("server sends garbage (not valid server)", "", err, code,
      expect_stderr='pubsubclient: server at "localhost:19998" is not a valid server',
      expect_exit=8)

# Test: server accepts but says nothing
t = threading.Thread(target=fake_server, args=(19997, "silent"), daemon=True)
t.start()
time.sleep(0.2)
_, err, code = run([":19997", "c1"], timeout=4)
check("server silent (not valid server)", "", err, code,
      expect_stderr='pubsubclient: server at "localhost:19997" is not a valid server',
      expect_exit=8)

# ─────────────────────────────────────────────
# 7. INTERACTIVE MODE STDIN COMMANDS (no server needed for parsing)
#    These require a working server - skipped here unless you have one running
# ─────────────────────────────────────────────
print("\n=== Summary ===")
print(f"PASS: {PASS}  FAIL: {FAIL}  TOTAL: {PASS+FAIL}")