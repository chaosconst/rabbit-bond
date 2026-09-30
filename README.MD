# Rabbit Bond

Many paths bonded into one connection: a multipath TCP / UDP forwarder

English | [中文](README_ZH.MD)

## Many paths, one connection

```console
$ curl --http1.1 -x socks5h://127.0.0.1:16337 -o /dev/null https://speedtest.tokyo2.linode.com/1GB-tokyo2.bin
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 1000M  100 1000M    0     0  53.0M      0  0:00:18  0:00:18 --:--:-- 84.4M
```

Beijing to Tokyo, over 2000 km across the sea (2093 km straight line): **one TCP connection, 1 GB in 18 seconds**. This is a curl run from everyday use; the output is unedited. The local end is a home Wi-Fi broadband line plus a phone's 5G; the far end is a cloud server in Tokyo.

**Bandwidth is set by the two ends. The middle is rabbit's.**

- Your end: the sum of every line you have. Wi-Fi plus 5G means both added together.
- The far end: the target's own speed. Put the server next to the target, and everything in between is rabbit's.
- The middle: crossing seas, detours, loss, jitter, a path suddenly dropping out. All of it is rabbit's job.

Typical use: faster access to your own servers abroad, such as cross-border data sync, remote development, games and video calls.

Measured (2026-09-28 to 09-30):

- One connection peaked at **831 Mbit/s** (average after the first 2 s of ramp-up; 795 Mbit/s over the full 20 s).
- Steady-state efficiency **above 100%**. Efficiency = one connection's speed ÷ total capacity measured in the same time window with a dozen-plus parallel connections on each path; steady state = excluding the first 2 s of ramp-up.
  - Wi-Fi + 5G: 4 rounds interleaved with the previous version, 97% overall, **102%** steady.
  - Adding a Shanghai relay as a third path: up to **109%** steady.
  
  In other words, one rabbit connection beats a dozen-plus parallel downloads on every path combined.
- Any path can drop mid-transfer; the connection survives, with a 1–2 s dip in speed.
- New-connection median time to first byte is **98 ms**, faster than connecting directly to the same server (121 ms).
- Small game-like UDP packets: median 66 ms, p95 **83 ms**, no loss. Over a single relay path: median 84–110 ms, p95 about 100 ms.

Caveats: failover costs a 1–2 s dip; long-running high-speed transfers have not been tested under every condition. The test file is Linode's public speed-test file, so anyone can reproduce it:
`curl --http1.1 -o /dev/null https://speedtest.tokyo2.linode.com/1GB-tokyo2.bin`

### Why not an existing solution

- **MPTCP** needs kernel support on both ends, or an aggregation proxy in between. Each path must be a network interface or WAN port, and relays strip the MPTCP options. rabbit runs in user space: any TCP channel that reaches the server can be a path.
- **UDP-based tunnel accelerators**: many networks throttle long, heavy UDP flows. Every rabbit path is plain TCP and can be wrapped in any transport (TLS, for example).
- **A single proxy or relay**: speed and reliability hang on that one path. When it slows down or drops, so does the connection.

### What it adds to upstream (Rabbit TCP)

- **Per-tunnel acks and flow control** (`-flow`, both peers). Bandwidth is estimated from per-block samples, and each window follows its own path's speed, so fast paths aren't held back by slow ones. Head-of-line holes are repaired with NACKs, and blocks on a stalled tunnel are requeued.
- **Earliest-arrival scheduling**: each block goes to the tunnel expected to deliver it first. Paths that are often late get less (`RABBIT_FLOW_LATE_MS`, `RABBIT_FLOW_SCHED_QUEUE`).
- **Multiple endpoints**: `-rabbit-addr a*2,b*1,c*1` spreads tunnels over several addresses by weight; each address is one path. A dead endpoint backs off for 10 s, and each endpoint is capped at its share, so it can't starve the others.
- **Head duplication**: the first 6 blocks of every connection are sent on two exit groups, and the first copy wins (`RABBIT_FLOW_DUP_FIRST=6`; mark exit groups with `RABBIT_FLOW_LINES`, where paths leaving through the same exit form one group). The request behind a single click is usually just these few blocks, so it no longer gambles on landing on a slow path.
- **socks5**: the client answers the greeting and CONNECT locally, saving two round trips per new connection (`RABBIT_SOCKS_LOCAL_CONNECT=1`). Native UDP ASSOCIATE (`RABBIT_SOCKS_UDP=1`): datagrams under 600 bytes are sent on two exit groups and deduplicated by sequence number (`RABBIT_FLOW_DUP_DGRAM=600`).
- Optional AES-128-GCM (`RABBIT_CIPHER=AES-128-GCM`), cheaper on CPUs with hardware AES.

### Example

Speeding up SSH to your own server. Local ports 12455, 12456 and 12457 are each a TCP channel to the server's `:9891` (direct, phone hotspot, via a relay, anything). The first two leave through the same exit:

```sh
# server
export RABBIT_CIPHER=AES-128-GCM RABBIT_FLOW_LATE_MS=300 RABBIT_FLOW_SCHED_QUEUE=1
export RABBIT_FLOW_DUP_FIRST=6 RABBIT_FLOW_DUP_DGRAM=600
rabbit -mode s -flow -password "$PASSWORD" -rabbit-addr :9891

# client
export RABBIT_CIPHER=AES-128-GCM RABBIT_FLOW_LATE_MS=300 RABBIT_FLOW_SCHED_QUEUE=1
export RABBIT_FLOW_DUP_FIRST=6 RABBIT_FLOW_DUP_DGRAM=600
export RABBIT_FLOW_LINES="127.0.0.1:12455=a,127.0.0.1:12456=a,127.0.0.1:12457=b"
rabbit -mode c -flow -password "$PASSWORD" -tunnelN 16 \
  -rabbit-addr "127.0.0.1:12455*2,127.0.0.1:12456*1,127.0.0.1:12457*1" \
  -listen 127.0.0.1:2222 -dest 127.0.0.1:22   # dest: the service to speed up, on the server side

# use it
rsync -e "ssh -p 2222" -a ./data/ user@127.0.0.1:/srv/data/
```

Put any service's address in `-dest` to speed up that service. The client can also be a socks5 entry for both TCP and UDP (`RABBIT_SOCKS_LOCAL_CONNECT=1`, `RABBIT_SOCKS_UDP=1`); `-dest` is then a socks5 service on the server side. Live state (per-tunnel rate, RTT, duplication counters) is at `/debug/flow` on the address given by `RABBIT_PPROF`.

---

The upstream documentation follows.

## Introduction

This project is created to support and forward TCP connections. It will split bytes into blocks and reconstruct them during forwarding.
All upper connections visible to all users are carried by `N` underlying physical connections, and even a single upper connection is forwarded through all underlying connections.

Due to the use of multiple connections, similar to the multi-threaded downloader, this project can accelerate the connection without any more traffic consumption (bilateral deployment is required).
This project is a fork of [ihciah/rabbit-tcp](https://github.com/ihciah/rabbit-tcp). Thanks to the original author.

![Rabbit Bond](.github/resources/rabbit-tcp.svg)

## Get Started
This project can work in standalone proxy mode or inline in other Golang code.

Here is the common usage. For detailed documentation, please go to the [Wiki](https://github.com/ihciah/rabbit-tcp/wiki).

### Accelerate any TCP service in a standalone proxy mode
#### Server side
1. Install [Docker](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-convenience-script) and [docker-compose](https://docs.docker.com/compose/install/)
2. Download and edit `docker-compose-server.yml` ([Link](https://github.com/ihciah/rabbit-tcp/raw/master/docker-compose-server.yml))
    ```yaml
    rabbit-server:
      image: ihciah/rabbit
      ports:
        - "$RABBIT_PORT:9891/tcp"
      environment:
        - MODE=s
        - PASSWORD=$RABBIT_PASSWORD
        - RABBITADDR=:9891
        - VERBOSE=2
      restart: always
    ```
   - `$RABBIT_PORT` replaced with RabbitTCP service port, which should be the same as the client side
   - `$RABBIT_PASSWORD`  replaced with RabbitTCP service password, which should be the same as the client side
3. Run in the same directory `docker-compose -f docker-compose-server.yml up -d`

#### Client side
1. Install [Docker](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-convenience-script) and [docker-compose](https://docs.docker.com/compose/install/)
2. Download and edit `docker-compose-client.yml` ([Link](https://github.com/ihciah/rabbit-tcp/raw/master/docker-compose-client.yml))
    ```yaml
    rabbit-client:
      image: ihciah/rabbit
      ports:
        - "$CLIENT_LISTEN_PORT:9892/tcp"
      environment:
        - MODE=c
        - PASSWORD=$RABBIT_PASSWORD
        - RABBITADDR=$RABBIT_ADDR
        - LISTEN=:9892
        - DEST=$SERVICE_ADDR
        - TUNNELN=6
        - VERBOSE=2
      restart: always
    ```
   - `$RABBIT_ADDR` replaced with `ip_or_domain:port` , which should be the same as the server side
   - `$RABBIT_PASSWORD` replaced with RabbitTCP service password, which should be the same as the server side
   - `$SERVICE_ADDR` replaced with the `ip_or_domain:port` of the target service
   - `$CLIENT_LISTEN_PORT` replaced with local listen port, which can be dialed when connecting to the target service with acceleration.
   - Modify `TUNNELN` if you want to change count of physical connections
3. Run in the same directory `docker-compose -f docker-compose-server.yml up -d`
