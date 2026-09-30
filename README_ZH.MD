# Rabbit TCP

![Build badge](https://github.com/ihciah/rabbit-tcp/workflows/Build/badge.svg) ![Docker pull badge](https://img.shields.io/docker/pulls/ihciah/rabbit)

一个为加速而生的多连接 TCP 转发器

[English](README.MD) | 中文

## flow-control 分支：多条路合成一条连接

```console
$ curl --http1.1 -x socks5h://127.0.0.1:16337 -o /dev/null https://speedtest.tokyo2.linode.com/1GB-tokyo2.bin
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 1000M  100 1000M    0     0  53.0M      0  0:00:18  0:00:18 --:--:-- 84.4M
```

北京到东京，跨海 2000 多公里（直线 2093 km），**一条 TCP 连接，1GB 用了 18 秒**。这是日常使用中随手跑的一次 curl，输出原样贴出。本地是家里的 Wi-Fi 宽带加一部手机的 5G，远端是东京一台云服务器。

**带宽由两端决定，中间交给 rabbit。**

- 这一端：你手里所有线路加起来的速度。Wi-Fi 加 5G，就是两者之和。
- 那一端：目标自己的速度。服务器放在目标旁边，中间就全归 rabbit 管。
- 中间：跨海、绕路、丢包、抖动、某条路突然断掉，全部交给 rabbit。

典型用途：加速访问你自己的海外服务器，比如跨境数据同步、远程开发、游戏和视频会议。

实测（2026-09-28 至 09-30）：

- 单连接最高 **831 Mbit/s**（去掉起步 2 秒后的平均；含起步的 20 秒平均 795 Mbit/s）。
- 稳态效率**超过 100%**。效率 = 单连接速度 ÷ 同一时段每条路各开十几条并行连接测出的总容量；稳态 = 去掉起步的头 2 秒。
  - Wi-Fi 加 5G：新旧版本同时段交替各测 4 轮，整体 97%，稳态 **102%**。
  - 再加一条经上海的中转，三路：稳态最高 **109%**。
  
  也就是说，rabbit 的一条连接，比每条路各开十几条连接并行下载加起来还快。
- 传输中任意一条路断线，连接不断，速度掉 1–2 秒后恢复。
- 新连接首字节中位 **98 ms**，比直接连同一台服务器（121 ms）还快。
- 游戏类小 UDP 包：中位 66 ms，p95 **83 ms**，0 丢包；只走一条中转路时，中位 84–110 ms，p95 约 100 ms。

说明：断线切换时有 1–2 秒降速；长时间高速传输没有穷尽所有情况测试。测试文件是 Linode 的公开测速文件，任何人都能复现：
`curl --http1.1 -o /dev/null https://speedtest.tokyo2.linode.com/1GB-tokyo2.bin`

### 为什么不用现成方案

- **MPTCP**：两端内核都要支持，或者在中间架一个聚合代理；每条路必须是一块网卡或一个 WAN 口，经过中转节点时 MPTCP 选项会被剥掉。rabbit 跑在用户态，任何一条能到服务器的 TCP 通道都能当一条路。
- **基于 UDP 的隧道加速**：很多网络会对长时间的大流量 UDP 限速。rabbit 的每条路都是普通 TCP，外面还能再套任意传输层（比如 TLS）。
- **单条代理或中转**：速度和稳定性都系于那一条路，它慢了或断了，连接就跟着慢、跟着断。

### 相对上游改了什么

- **每条子连接独立确认、独立流控**（`-flow`，两端都要开）：按块采样估带宽，窗口跟各自的速度走，快路不会被慢路拖住。队头出现空洞时用 NACK 补发，卡住的子连接上的块重新排队。
- **按"最早到达"调度**：估算每条子连接送达这一块的时间，交给最早的那条；经常迟到的路少分（`RABBIT_FLOW_LATE_MS`、`RABBIT_FLOW_SCHED_QUEUE`）。
- **多地址**：`-rabbit-addr a*2,b*1,c*1` 按权重把子连接分给多个地址，每个地址就是一条路。死掉的路退避 10 秒；每条路只占自己的份额，不会饿死别的路。
- **头部双发**：每个连接的头 6 块在两个出口组各发一份，先到先用（`RABBIT_FLOW_DUP_FIRST=6`；出口组用 `RABBIT_FLOW_LINES` 标注，同一出口的路算一组）。点一下按钮产生的请求通常就这几块，不用再赌它落在慢路上。
- **socks5**：客户端本地应答握手和 CONNECT，新连接少两个来回（`RABBIT_SOCKS_LOCAL_CONNECT=1`）。原生 UDP ASSOCIATE（`RABBIT_SOCKS_UDP=1`）：600 字节以下的数据报在两个出口组各发一份，按序号去重（`RABBIT_FLOW_DUP_DGRAM=600`）。
- 可选 AES-128-GCM（`RABBIT_CIPHER=AES-128-GCM`），有硬件 AES 的 CPU 上更省。

### 用法示例

加速访问你自己的服务器上的 SSH。本地 12455、12456、12457 各是一条通往服务器 `:9891` 的 TCP 通道（直连、手机热点、经中转都行），前两条从同一个出口出去：

```sh
# 服务端
export RABBIT_CIPHER=AES-128-GCM RABBIT_FLOW_LATE_MS=300 RABBIT_FLOW_SCHED_QUEUE=1
export RABBIT_FLOW_DUP_FIRST=6 RABBIT_FLOW_DUP_DGRAM=600
rabbit -mode s -flow -password "$PASSWORD" -rabbit-addr :9891

# 客户端
export RABBIT_CIPHER=AES-128-GCM RABBIT_FLOW_LATE_MS=300 RABBIT_FLOW_SCHED_QUEUE=1
export RABBIT_FLOW_DUP_FIRST=6 RABBIT_FLOW_DUP_DGRAM=600
export RABBIT_FLOW_LINES="127.0.0.1:12455=a,127.0.0.1:12456=a,127.0.0.1:12457=b"
rabbit -mode c -flow -password "$PASSWORD" -tunnelN 16 \
  -rabbit-addr "127.0.0.1:12455*2,127.0.0.1:12456*1,127.0.0.1:12457*1" \
  -listen 127.0.0.1:2222 -dest 127.0.0.1:22   # dest：服务端一侧要加速的服务

# 使用
rsync -e "ssh -p 2222" -a ./data/ user@127.0.0.1:/srv/data/
```

`-dest` 换成任何服务的地址，就是加速那个服务。客户端也可以当 socks5 入口，同时支持 TCP 和 UDP（`RABBIT_SOCKS_LOCAL_CONNECT=1`、`RABBIT_SOCKS_UDP=1`），这时 `-dest` 填服务端一侧的 socks5 服务。运行状态（每条子连接的速率、RTT、双发计数）见 `RABBIT_PPROF` 指定地址下的 `/debug/flow`。

---

以下是上游原有说明。

## 简介

这个项目用来承载并转发 TCP 流量，并在转发时对流量进行分拆与重组。
所有用户可见的上层连接均由 `N` 条底层物理连接承载，即便是单条用户连接也会经所有底层连接进行转发。

由于使用了多条连接，与多线程下载器类似，本项目可以在不多消耗流量的情况下对连接进行加速（需双边部署）。
本项目 fork 自 [ihciah/rabbit-tcp](https://github.com/ihciah/rabbit-tcp)，感谢原作者。

![Rabbit TCP](.github/resources/rabbit-tcp.svg)

## 开始使用
本项目既可以以独立代理模式工作，也可以内嵌在其他 Golang 代码中提供服务。

以下是常用用法，详细的文档请移步 [Wiki](https://github.com/ihciah/rabbit-tcp/wiki)。

### 以独立代理模式加速任意 TCP 服务
#### 服务端
1. 安装 [Docker](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-convenience-script) 和 [docker-compose](https://docs.docker.com/compose/install/)
2. 下载并修改 `docker-compose-server.yml` 文件([链接](https://github.com/ihciah/rabbit-tcp/raw/master/docker-compose-server.yml))
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
   - `$RABBIT_PORT` 替换为 RabbitTCP 服务端口，与 Client 保持一致即可
   - `$RABBIT_PASSWORD` 替换为 RabbitTCP 服务密码，与 Client 保持一致即可
3. 在该文件同目录运行 `docker-compose -f docker-compose-server.yml up -d`

#### 客户端
1. 安装 [Docker](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-convenience-script) 和 [docker-compose](https://docs.docker.com/compose/install/)
2. 下载并修改 `docker-compose-client.yml` 文件([链接](https://github.com/ihciah/rabbit-tcp/raw/master/docker-compose-client.yml))
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
   - `$RABBIT_ADDR` 替换为 RabbitTCP 服务 `地址:端口` ，端口与服务端保持一致即可
   - `$RABBIT_PASSWORD` 替换为 RabbitTCP 服务密码，与服务端保持一致即可
   - `$SERVICE_ADDR` 替换为待加速的服务 `地址:端口` 
   - `$CLIENT_LISTEN_PORT` 替换为本地监听端口，后续若要加速访问目标服务，连接该端口即可
   - 修改 `TUNNELN` 可以控制底层物理连接数
3. 在该文件同目录运行 `docker-compose -f docker-compose-server.yml up -d`
