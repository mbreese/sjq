import sys
import time
import socket

def readline(sock, timeout=30, verbose=False, logger=None):
    s = ""
    start = time.time()
    while not s or s[-1] != '\n':
        try:
            ch = sock.recv(1)
            if len(ch) > 0:
                start = time.time()
                s += ch
            else:
                now = time.time()
                if now - start < timeout:
                    time.sleep(0.1)
                else:
                    if logger:
                        logger("<<< <timeout reading>")
                    elif verbose:
                        sys.stderr.write("<<< <timeout reading>\n")
                    return None

        except socket.timeout:
            if logger:
                logger("<<< <timeout reading>")
            elif verbose:
                sys.stderr.write("<<< <timeout reading>\n")
            return None
        except socket.error, e:
            if e.args[0] == 35:
                now = time.time()
                if now - start < timeout:
                    time.sleep(0.1)
                else:
                    if logger:
                        logger("<<< <timeout reading>")
                    elif verbose:
                        sys.stderr.write("<<< <timeout reading>\n")
                    return None
            else:
                return None

    if logger:
        logger("<<< %s" % (s.replace('\n', '\\n').replace('\r', '\\r')))
    elif verbose:
        sys.stderr.write("<<< %s\n" % (s.replace('\n', '\\n').replace('\r', '\\r')))

    return s.rstrip('\r\n')

