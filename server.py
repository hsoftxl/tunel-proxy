#!/usr/bin/env python3
"""
内网穿透 - 服务端
运行在有公网 IP 的 Linux 服务器上，支持多端口映射和客户端重连
"""

import socket
import select
import threading
import uuid
import time
import logging
from concurrent.futures import ThreadPoolExecutor

from tunnel_common import (
    BandwidthMonitor,
    set_tcp_keepalive,
    read_line,
    tunnel_forward,
    start_bw_reporter,
    validate_port,
)

logger = logging.getLogger(__name__)

# 控制通道心跳超时（秒）——超过此时间未收到 PING 就判定客户端失联
CLIENT_HEARTBEAT_TIMEOUT = 60
# Pending 连接最大等待时间（秒）
PENDING_TIMEOUT = 30
# Pending 清理线程检查间隔（秒）
PENDING_CLEAN_INTERVAL = 10


class TunnelServer:
    def __init__(self, control_port=9000, data_port=9001,
                 client_listen_backlog=5):
        self.control_port = control_port
        self.data_port = data_port
        self.client_listen_backlog = client_listen_backlog
        self.pending = {}          # conn_id -> (socket, timestamp)
        self.lock = threading.Lock()
        self.ctrl_conn = None
        self.port_map = {}
        self.pub_sockets = {}
        self.dat = None
        self.ctr = None
        self.bw = BandwidthMonitor()
        # 统一线程池：数据转发线程 + 监听线程
        self.executor = ThreadPoolExecutor(
            max_workers=50,
            thread_name_prefix="tunnel"
        )
        self._running = False
        self._cleaner_started = False

    def _clean_stale_pending(self):
        """后台线程：定期清理过期的 pending 连接"""
        while self._running:
            time.sleep(PENDING_CLEAN_INTERVAL)
            now = time.time()
            stale = []
            with self.lock:
                for cid, (conn, ts) in list(self.pending.items()):
                    if now - ts > PENDING_TIMEOUT:
                        stale.append(cid)
                for cid in stale:
                    conn, _ = self.pending.pop(cid)
                    try:
                        conn.close()
                    except OSError:
                        pass
            if stale:
                logger.warning("Cleaned %d stale pending connections", len(stale))

    def forward(self, a, b):
        """使用共享转发函数，并记录带宽"""
        tunnel_forward(a, b, bw_monitor=self.bw)

    def handle_data_conn(self, conn):
        try:
            conn_id = read_line(conn)
            if conn_id is None:
                conn.close()
                return

            with self.lock:
                entry = self.pending.pop(conn_id, None)

            if entry:
                ext_conn, _ = entry
                self.forward(ext_conn, conn)
            else:
                logger.warning("Unknown data conn id=%s, closing", conn_id)
                conn.close()
        except Exception:
            conn.close()

    def bind_pub_port(self, remote_port):
        if not validate_port(remote_port):
            logger.error("Invalid port: %d", remote_port)
            return False
        try:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # macOS 下 SO_REUSEPORT 可选
            srv.bind(('0.0.0.0', remote_port))
            srv.listen(100)
            self.pub_sockets[remote_port] = srv
            logger.info("Bound :%d", remote_port)
            return True
        except Exception as e:
            logger.error("Bind :%d failed: %s", remote_port, e)
            return False

    def start_pub_listeners(self):
        start_bw_reporter(self.bw)

        for remote_port, srv in list(self.pub_sockets.items()):
            def accept_loop(s=srv, rp=remote_port, ctrl=self.ctrl_conn):
                while self._running:
                    try:
                        ext_conn, addr = s.accept()
                        set_tcp_keepalive(ext_conn)
                        conn_id = uuid.uuid4().hex[:8]
                        with self.lock:
                            self.pending[conn_id] = (ext_conn, time.time())
                        try:
                            ctrl.sendall(
                                f"NEW:{conn_id}:{rp}\n".encode())
                        except (ConnectionError, OSError):
                            return
                        logger.info(":%d <- %s:%s  id=%s",
                                    rp, addr[0], addr[1], conn_id)
                    except OSError:
                        break

            self.executor.submit(accept_loop)

    def cleanup(self):
        logger.info("Cleaning up...")
        for s in self.pub_sockets.values():
            try:
                s.close()
            except Exception:
                pass
        self.pub_sockets.clear()
        self.port_map.clear()
        # 清理所有 pending 连接
        with self.lock:
            for conn, _ in self.pending.values():
                try:
                    conn.close()
                except OSError:
                    pass
            self.pending.clear()
        if self.ctrl_conn:
            try:
                self.ctrl_conn.close()
            except Exception:
                pass
            self.ctrl_conn = None
        logger.info("Cleanup done, waiting for new client...")

    def handle_client(self, conn, addr):
        # 进入 handle_client 时状态已经由调用方 (run 中的 cleanup) 清理干净
        set_tcp_keepalive(conn)
        self.ctrl_conn = conn
        logger.info("Client connected from %s", addr)
        conn.sendall(b"OK\n")

        logger.info("Waiting for port mappings...")
        buf = b''
        mapping_done = False
        while not mapping_done:
            try:
                data = conn.recv(4096)
            except (ConnectionError, OSError):
                logger.error("Client disconnected during mapping")
                return False
            if not data:
                logger.error("Client disconnected during mapping")
                return False
            buf += data
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                try:
                    msg = line.decode().strip()
                except UnicodeDecodeError:
                    logger.warning("Invalid data from %s, skipping", addr)
                    continue
                if msg.startswith('MAP:'):
                    parts = msg[4:].split(':')
                    if len(parts) == 2:
                        remote_port = int(parts[0])
                        local_port = int(parts[1])
                        if not validate_port(remote_port) or not validate_port(local_port):
                            logger.error("Invalid port in mapping: :%d -> :%d",
                                         remote_port, local_port)
                            continue
                        self.port_map[remote_port] = local_port
                        ok = self.bind_pub_port(remote_port)
                        if not ok:
                            logger.warning("Port :%d mapping failed, removing",
                                           remote_port)
                            self.port_map.pop(remote_port, None)
                        else:
                            logger.info("Map: :%d -> client:%d",
                                        remote_port, local_port)
                elif msg == 'MAP_DONE':
                    logger.info("Total %d port(s) mapped", len(self.port_map))
                    if not self.port_map:
                        logger.error("No ports mapped successfully")
                        try:
                            conn.sendall(b"MAP_FAIL:no_ports_bound\n")
                        except (ConnectionError, OSError):
                            pass
                        return False
                    try:
                        conn.sendall(b"MAP_OK\n")
                    except (ConnectionError, OSError):
                        return False
                    self.start_pub_listeners()
                    mapping_done = True

        # 启动 pending 清理后台线程（只启动一次，避免重连后重复创建）
        self._running = True
        if not self._cleaner_started:
            self._cleaner_started = True
            threading.Thread(target=self._clean_stale_pending, daemon=True).start()

        # 控制通道主循环
        last_activity = time.time()
        buf = b''
        while True:
            try:
                readable, _, exceptional = select.select(
                    [conn], [], [conn], 10)
            except (ValueError, OSError):
                break

            if exceptional:
                logger.error("Client control socket error")
                return False

            if not readable:
                # select 超时：检查心跳是否超时
                if time.time() - last_activity > CLIENT_HEARTBEAT_TIMEOUT:
                    logger.error("Client heartbeat timeout")
                    return False
                continue

            try:
                data = conn.recv(4096)
            except (ConnectionError, OSError):
                logger.error("Client disconnected (connection error)")
                return False

            if not data:
                logger.error("Client disconnected")
                return False

            last_activity = time.time()
            buf += data
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                msg = line.decode().strip()
                if msg == 'PING':
                    try:
                        conn.sendall(b"PONG\n")
                    except (ConnectionError, OSError):
                        logger.error("Failed to send PONG")
                        return False
                elif msg.startswith('NEW:'):
                    # 旧协议残留
                    parts = msg[4:].split(':')
                    conn_id = parts[0]
                    remote_port = int(parts[1])
                    logger.info("Tunnel (ctrl legacy): :%d id=%s",
                                remote_port, conn_id)
                    with self.lock:
                        entry = self.pending.pop(conn_id, None)
                    if entry:
                        ext_conn, _ = entry
                        self.executor.submit(self.handle_data_conn, ext_conn)
                else:
                    logger.warning("Unknown control msg: %s", msg)

    def run(self):
        self.dat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dat.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.dat.bind(('0.0.0.0', self.data_port))
        self.dat.listen(10)

        self.ctr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ctr.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ctr.bind(('0.0.0.0', self.control_port))
        self.ctr.listen(self.client_listen_backlog)

        def data_acceptor():
            while self._running:
                try:
                    c, _ = self.dat.accept()
                    self.executor.submit(self.handle_data_conn, c)
                except OSError:
                    break
                except Exception:
                    logger.exception("Data acceptor error, restarting")
                    continue

        self._running = True
        threading.Thread(target=data_acceptor, daemon=True).start()

        logger.info("Control port: %d", self.control_port)
        logger.info("Data port:    %d", self.data_port)
        logger.info("Waiting for client...")

        try:
            while True:
                conn, addr = self.ctr.accept()
                self.handle_client(conn, addr)
                # 每次客户端断开后统一清理状态，下次 accept 前状态一定是干净的
                self.cleanup()
        except KeyboardInterrupt:
            logger.info("Shutting down")
        except OSError:
            pass
        finally:
            self._running = False
            self.cleanup()
            self.executor.shutdown(wait=False)

        self.dat.close()
        self.ctr.close()


if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    parser = argparse.ArgumentParser(description='内网穿透服务端')
    parser.add_argument('--control-port', type=int, default=9000,
                        help='控制端口 (默认 9000)')
    parser.add_argument('--data-port', type=int, default=9001,
                        help='数据端口 (默认 9001)')
    parser.add_argument('--heartbeat-timeout', type=int, default=60,
                        help='客户端心跳超时秒数 (默认 60)')
    parser.add_argument('--client-listen', type=int, default=5,
                        help='控制端口 backlog (默认 5)')
    args = parser.parse_args()

    if args.heartbeat_timeout:
        CLIENT_HEARTBEAT_TIMEOUT = args.heartbeat_timeout

    TunnelServer(
        control_port=args.control_port,
        data_port=args.data_port,
        client_listen_backlog=args.client_listen,
    ).run()
