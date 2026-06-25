**English** | [中文](./README.md)

# Intranet Penetration (内网穿透)

A lightweight TCP intranet penetration tool that exposes local services behind NAT to the public internet, with multi-port mapping and automatic reconnection.

## Architecture

```
Public Server (Server)                 Private Network (Client)
┌────────────────────┐           ┌─────────────────────────┐
│  Control :9000     │◄──────────│  Control channel (PING)  │
│  Data    :9001     │◄──────────│  Data channel (tunnel)   │
│  :8080  ← mapped   │           │  localhost:8003          │
│  :3000  ← mapped   │           │  localhost:3000          │
└────────────────────┘           └─────────────────────────┘
```

- **Control channel**: A persistent TCP connection between client and server for heartbeats (PING/PONG) and tunnel signaling (NEW messages).
- **Data channel**: Each external connection creates an independent TCP tunnel, forwarded through the server to the client.
- **Port mapping**: The client registers `public_port:local_port` mappings; external traffic is transparently tunneled to the internal service.

## Quick Start

### 1. Server (public server)

```bash
# Default: control port 9000, data port 9001
python3 server.py

# Custom ports
python3 server.py --control-port 9000 --data-port 9001
```

### 2. Client (private network machine)

```bash
# Map a single port
python3 client.py <SERVER_IP> -p 8080:8003

# Map multiple ports
python3 client.py <SERVER_IP> -p 8080:8003 -p 3000:3000
```

### Example

Expose a local web service on port 8003 through the server's port 8080:

```bash
# Server
python3 server.py

# Client
python3 client.py 1.2.3.4 -p 8080:8003
```

Visit `http://1.2.3.4:8080` to reach the local service at port 8003.

## CLI Arguments

### Server

| Argument | Default | Description |
|---|---|---|
| `--control-port` | 9000 | Control channel port |
| `--data-port` | 9001 | Data channel port |
| `--heartbeat-timeout` | 60 | Client heartbeat timeout (seconds) |
| `--client-listen` | 5 | Control socket listen backlog |

### Client

| Argument | Default | Description |
|---|---|---|
| `server` | — | Server IP address (required) |
| `-p` / `--port` | — | Port mapping `remote:local` (repeatable) |
| `--control-port` | 9000 | Server control port |
| `--data-port` | 9001 | Server data port |
| `--tunnel-idle-timeout` | 0 | Tunnel idle timeout in seconds, 0 = never |

## File Structure

```
├── server.py         # Server (public internet)
├── client.py         # Client (private network)
├── tunnel_common.py  # Shared code
├── README.md         # Chinese documentation
└── README_EN.md      # English documentation
```

## Features

- **Multi-port mapping**: Map any number of ports on a single connection
- **Auto-reconnect**: Exponential backoff reconnection (1s → 2s → 4s → ... → 60s max)
- **Heartbeat keepalive**: TCP keepalive + application-level PING/PONG for reliable connection monitoring
- **Bandwidth monitoring**: Real-time throughput display
- **Idle timeout**: Configurable auto-close for idle tunnels
- **Thread pool**: Controlled concurrency to prevent resource exhaustion
- **Pending cleanup**: Automatic cleanup of stale pending connections to prevent socket leaks
- **Stale port recovery**: Automatic retry with `SO_REUSEPORT` when port binding fails after a reconnect

## Notes

- The server requires a public IP address; ensure firewalls allow the control and data ports
- Data is transmitted in plaintext; for sensitive services, consider using SSH tunneling or TLS alongside this tool
- The server supports a single client at a time (one-to-one deployment)
