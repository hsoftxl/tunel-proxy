#!/usr/bin/env python3
"""
内网穿透 - 客户端
运行在本地机器上，支持多端口映射和自动重连
"""

import socket
import threading
import select
import sys
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

# 控制通道心跳间隔（秒）
HEARTBEAT_INTERVAL = 25
# 初始握手超时（秒）
HANDSHAKE_TIMEOUT = 15
# 最大重连间隔（秒）
MAX_RECONNECT_DELAY = 60


class TunnelClient:
    def __init__(self, server_host, control_port=9000, data_port=9001,
                 port_map=None, tunnel_idle_timeout=0):
        self.server_host = server_host
        self.control_port = control_port
        self.data_port = data_port
        self.port_map = port_map or {}
        self.bw = BandwidthMonitor()
        self.tunnel_idle_timeout = tunnel_idle_timeout  # 0 = 永不超时
        self.executor = ThreadPoolExecutor(
            max_workers=50,
            thread_name_prefix="tunnel-cli"
        )

    def forward(self, a, b):
        idle = self.tunnel_idle_timeout if self.tunnel_idle_timeout > 0 else None
        tunnel_forward(a, b, bw_monitor=self.bw, idle_timeout=idle)

    def handle_new_conn(self, conn_id, remote_port):
        local_port = self.port_map.get(remote_port)
        if not local_port:
            logger.error("No mapping for remote port %d", remote_port)
            return
        # 先连接本地服务，失败就不用浪费服务端的数据连接
        local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_sock.settimeout(5)
        try:
            local_sock.connect(('127.0.0.1', local_port))
        except Exception as e:
            local_sock.close()
            logger.error("Tunnel %s -> 127.0.0.1:%d failed: %s",
                         conn_id, local_port, e)
            return
        local_sock.settimeout(None)

        try:
            data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            set_tcp_keepalive(data_sock)
            data_sock.settimeout(10)
            data_sock.connect((self.server_host, self.data_port))
            data_sock.settimeout(None)
            data_sock.sendall(f"{conn_id}\n".encode())
        except Exception as e:
            local_sock.close()
            data_sock.close()
            logger.error("Tunnel %s data conn failed: %s", conn_id, e)
            return

        self.forward(data_sock, local_sock)

    def connect_once(self):
        ctrl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_tcp_keepalive(ctrl)
        ctrl.settimeout(10)
        try:
            ctrl.connect((self.server_host, self.control_port))
        except (ConnectionError, OSError) as e:
            logger.error("Connect failed: %s", e)
            ctrl.close()
            return False
        ctrl.settimeout(None)

        # 使用带超时的 read_line 读取初始握手和 MAP_OK
        hello = read_line(ctrl, timeout=HANDSHAKE_TIMEOUT)
        if hello is None:
            logger.error("No response from server (timeout)")
            ctrl.close()
            return False
        if hello != "OK":
            logger.error("Unexpected handshake: %s", hello)
            ctrl.close()
            return False

        logger.info("Connected to %s:%d",
                    self.server_host, self.control_port)

        for remote_port, local_port in self.port_map.items():
            ctrl.sendall(f"MAP:{remote_port}:{local_port}\n".encode())
            logger.info("Mapping :%d -> localhost:%d", remote_port, local_port)

        ctrl.sendall(b"MAP_DONE\n")
        resp = read_line(ctrl, timeout=HANDSHAKE_TIMEOUT)
        if resp is None:
            logger.error("MAP_OK timeout")
            ctrl.close()
            return False
        if resp == "MAP_OK":
            pass  # 下方继续
        elif resp and resp.startswith("MAP_FAIL:"):
            reason = resp[9:]
            logger.error("Server rejected mappings: %s", reason)
            ctrl.close()
            return False
        else:
            logger.error("Server rejected mappings: %s", resp)
            ctrl.close()
            return False

        logger.info("All ports mapped, tunnel ready")
        start_bw_reporter(self.bw)

        # 控制通道主循环
        last_heartbeat = time.time()
        buf = b''
        while True:
            try:
                readable, _, exceptional = select.select(
                    [ctrl], [], [ctrl], HEARTBEAT_INTERVAL)
            except (ValueError, OSError):
                break

            if exceptional:
                logger.error("Control socket error")
                break

            now = time.time()

            if not readable:
                # select 超时 → 发送心跳
                if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                    try:
                        ctrl.sendall(b"PING\n")
                        last_heartbeat = now
                    except (ConnectionError, OSError):
                        logger.error("Heartbeat send failed")
                        break
                continue

            try:
                data = ctrl.recv(4096)
            except (ConnectionError, OSError):
                logger.error("Server disconnected (connection error)")
                break

            if not data:
                logger.error("Server disconnected")
                break

            buf += data
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                msg = line.decode().strip()
                if msg == 'PONG':
                    pass
                elif msg.startswith('NEW:'):
                    parts = msg[4:].split(':')
                    conn_id = parts[0]
                    remote_port = int(parts[1])
                    logger.info("Tunnel: :%d id=%s", remote_port, conn_id)
                    self.executor.submit(
                        self.handle_new_conn, conn_id, remote_port)
                else:
                    logger.warning("Unknown control msg: %s", msg)

        ctrl.close()
        return True

    def run(self):
        delay = 1  # 初始重连间隔（秒）
        normal_delay = 1  # 正常断开后也稍等再重连
        while True:
            try:
                ok = self.connect_once()
            except KeyboardInterrupt:
                logger.info("Shutting down")
                self.executor.shutdown(wait=False)
                break
            except Exception as e:
                logger.error("Connection error: %s", e)
                ok = False

            if ok:
                logger.info("Disconnected, reconnecting...")
                delay = normal_delay
            else:
                logger.info("Reconnecting in %ds...", delay)

            time.sleep(delay)
            # 指数退避，最大 MAX_RECONNECT_DELAY
            delay = min(delay * 2, MAX_RECONNECT_DELAY)


if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    parser = argparse.ArgumentParser(description='内网穿透客户端')
    parser.add_argument('server', help='服务器 IP')
    parser.add_argument('-p', '--port', action='append', dest='ports',
                        metavar='remote:local',
                        help='端口映射，可多次指定 (如 -p 8080:8003 -p 3000:3000)')
    parser.add_argument('--control-port', type=int, default=9000,
                        help='控制端口 (默认 9000)')
    parser.add_argument('--data-port', type=int, default=9001,
                        help='数据端口 (默认 9001)')
    parser.add_argument('--tunnel-idle-timeout', type=int, default=0,
                        help='隧道空闲超时(秒)，0=永不超时 (默认 0)')
    args = parser.parse_args()

    port_map = {}
    if args.ports:
        for p in args.ports:
            try:
                remote, local = p.split(':')
                rp, lp = int(remote), int(local)
                if not validate_port(rp) or not validate_port(lp):
                    logger.error("Invalid port mapping: %s", p)
                    sys.exit(1)
                port_map[rp] = lp
            except ValueError:
                logger.error("Invalid port format: %s (expected remote:local)", p)
                sys.exit(1)

    if not port_map:
        logger.error("At least one port mapping required (use -p remote:local)")
        sys.exit(1)

    TunnelClient(
        server_host=args.server,
        control_port=args.control_port,
        data_port=args.data_port,
        port_map=port_map,
        tunnel_idle_timeout=args.tunnel_idle_timeout,
    ).run()
