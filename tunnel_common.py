"""
内网穿透 - 共享模块
服务端和客户端共用代码
"""

import socket
import select
import threading
import time
import logging

logger = logging.getLogger(__name__)


class BandwidthMonitor:
    """简单的带宽监控器，线程安全"""
    def __init__(self):
        self.total_bytes = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def add(self, n: int):
        with self.lock:
            self.total_bytes += n

    def report(self) -> float:
        """返回平均带宽（字节/秒）"""
        with self.lock:
            elapsed = time.time() - self.start_time
            if elapsed == 0:
                return 0
            return self.total_bytes / elapsed

    def reset(self):
        with self.lock:
            self.total_bytes = 0
            self.start_time = time.time()


def set_tcp_keepalive(sock, idle=30, interval=5, count=3):
    """启用 TCP keepalive，快速检测死连接"""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # macOS: TCP_KEEPALIVE,  Linux: TCP_KEEPIDLE
    idle_opt = getattr(socket, 'TCP_KEEPALIVE',
                getattr(socket, 'TCP_KEEPIDLE', None))
    if idle_opt is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, idle_opt, idle)
        except OSError:
            pass
    # Linux: KEEPINTVL / KEEPCNT
    intvl_opt = getattr(socket, 'TCP_KEEPINTVL', None)
    cnt_opt = getattr(socket, 'TCP_KEEPCNT', None)
    if intvl_opt is not None and cnt_opt is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, intvl_opt, interval)
            sock.setsockopt(socket.IPPROTO_TCP, cnt_opt, count)
        except OSError:
            pass


def read_line(sock, timeout=None):
    """从 socket 读取一行（直到 \\n），连接断开或超时返回 None

    timeout: 秒数，None 表示无限等待
    """
    buf = b''
    deadline = time.time() + timeout if timeout else None
    while b'\n' not in buf:
        # 使用 select 实现超时
        readable, _, exceptional = select.select([sock], [], [sock],
                                                  timeout if timeout else None)
        if exceptional:
            return None
        if not readable:
            # select 超时
            return None
        try:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            buf += chunk
        except (ConnectionError, OSError):
            return None
        if deadline and time.time() >= deadline:
            return None
    return buf.decode().strip()


def tunnel_forward(a, b, bw_monitor=None, idle_timeout=None):
    """双向转发两个 socket 之间的数据

    a, b: 要转发的两个 socket 对象
    bw_monitor: 可选的 BandwidthMonitor，用于统计流量
    idle_timeout: 空闲超时秒数，None 表示无限等待
    """
    try:
        rlist = [a, b]
        while True:
            try:
                readable, _, exceptional = select.select(rlist, [], rlist,
                                                         idle_timeout)
            except (ValueError, OSError):
                break
            if exceptional:
                break
            if not readable:
                # idle timeout
                break
            for s in readable:
                try:
                    data = s.recv(65536)
                except (ConnectionError, OSError):
                    data = b''
                if not data:
                    return
                if bw_monitor is not None:
                    bw_monitor.add(len(data))
                try:
                    (b if s is a else a).sendall(data)
                except (ConnectionError, OSError):
                    return
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            a.close()
        except OSError:
            pass
        try:
            b.close()
        except OSError:
            pass


def start_bw_reporter(bw_monitor, interval=5, prefix="BW"):
    """启动带宽报告线程（daemon），每隔 interval 秒打印一次"""
    def _reporter():
        while True:
            time.sleep(interval)
            speed = bw_monitor.report()
            if speed > 0:
                if speed > 1024 * 1024:
                    logger.info(f"[{prefix}] {speed / 1024 / 1024:.1f} MB/s")
                elif speed > 1024:
                    logger.info(f"[{prefix}] {speed / 1024:.1f} KB/s")
                else:
                    logger.info(f"[{prefix}] {speed:.0f} B/s")
    t = threading.Thread(target=_reporter, daemon=True)
    t.start()
    return t


def validate_port(port: int) -> bool:
    """校验端口号是否在有效范围"""
    return 1 <= port <= 65535
