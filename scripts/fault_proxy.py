"""Loopback-only fault proxy for disposable test services; never a server API."""
import http.client
import http.server
import json
import secrets
import socket
import threading
import time
import urllib.parse


class FaultHTTPServer(http.server.ThreadingHTTPServer):
    # The extent runtime allows 200 concurrent downloads. Python's default
    # backlog of five resets/drops bursts before a handler can record them,
    # accidentally injecting object failures even with no fault configured.
    request_queue_size = 256


class FaultProxy:
    def __init__(self, upstream, *, objects=False):
        target = urllib.parse.urlsplit(upstream)
        self.mu = threading.Lock()
        self.rule = {}
        self.events = []
        self.release = threading.Event()
        self.token = secrets.token_hex(24)
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def handle(self):
                try:
                    super().handle()
                except (ConnectionResetError, BrokenPipeError):
                    pass  # expected when an injected fault aborts a client

            def body(self):
                if self.headers.get('Transfer-Encoding', '').lower() != 'chunked':
                    return self.rfile.read(int(self.headers.get('Content-Length', '0')))
                chunks = []
                while True:
                    size = int(self.rfile.readline().split(b';')[0], 16)
                    if not size:
                        while self.rfile.readline().strip():
                            pass
                        return b''.join(chunks)
                    chunks.append(self.rfile.read(size))
                    self.rfile.read(2)

            def respond(self, code, data, headers=()):
                self.send_response(code)
                for key, value in headers:
                    if key.lower() not in ('transfer-encoding', 'connection', 'content-length'):
                        self.send_header(key, value)
                length = next((v for k, v in headers if k.lower() == 'content-length'), str(len(data))) if self.command == 'HEAD' else str(len(data))
                self.send_header('Content-Length', length)
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(data)

            def handle_request(self):
                data = self.body()
                if self.path == '/_tidbfs_fault':
                    if self.headers.get('Authorization') != 'Bearer ' + owner.token:
                        self.respond(403, b'')
                        return
                    with owner.mu:
                        if self.command == 'POST':
                            owner.release.set()
                            owner.rule = json.loads(data)
                            owner.events = []
                            owner.release = threading.Event()
                        report = {'events': list(owner.events)}
                    self.respond(200, json.dumps(report).encode())
                    return
                op = self.headers.get('X-Drive9-Extent-Op', '')
                eligible = '/chunks/' in self.path if objects else bool(op)
                mode = None
                with owner.mu:
                    rule = owner.rule
                    release = owner.release
                    if eligible and self.command == rule.get('method') and (not rule.get('op') or op == rule['op']):
                        if rule.get('count', 0):
                            mode = rule.get('mode')
                            if rule['count'] > 0:
                                rule['count'] -= 1
                    event = {'method': self.command, 'path': self.path, 'op': op, 'fault': mode}
                    if op == 'setlk':
                        event['ltype'] = json.loads(data).get('ltype')
                    if eligible:
                        owner.events.append(event)
                if mode in ('unavailable', 'denied'):
                    code = 503 if mode == 'unavailable' else 403
                    kind = 'ServiceUnavailable' if code == 503 else 'AccessDenied'
                    event['status'] = code
                    self.respond(code, f'<Error><Code>{kind}</Code></Error>'.encode())
                    return
                connection = http.client.HTTPConnection(target.hostname, target.port, timeout=120)
                try:
                    # Preserve the original Host: it is part of the S3 signature.
                    headers = dict(self.headers)
                    for key in list(headers):
                        if key.lower() in ('transfer-encoding', 'connection'):
                            del headers[key]
                    headers['Content-Length'] = str(len(data))
                    connection.request(self.command, self.path, body=data, headers=headers)
                    response = connection.getresponse()
                    payload = response.read()
                    event['status'] = response.status
                    if mode == 'drop_ack' and 200 <= response.status < 300:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                    if mode == 'delay_ack' and 200 <= response.status < 300:
                        time.sleep(35)  # beyond the adapter's metadata deadline
                    if mode == 'hold_ack' and 200 <= response.status < 300:
                        release.wait(timeout=90)
                    if mode == 'short_read' and 200 <= response.status < 300:
                        self.send_response(response.status)
                        self.send_header('Content-Length', str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload[:len(payload) // 2])
                        self.wfile.flush()
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                    self.respond(response.status, payload, response.getheaders())
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    connection.close()

            do_GET = do_PUT = do_POST = do_HEAD = do_DELETE = handle_request

        self.server = FaultHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
