# Contiki-NG – Simple Sensing Application
(Group ID: XX)  
Members: Name Surname 1, Name Surname 2, ...

This project implements the “UDP sensing + alert” application described in the assignment. :contentReference[oaicite:0]{index=0}

## Client (udp_client.c)
- `get_temperature()` generates a fake temperature (one of `{30,25,20,15,10}`) using `random_rand()`.
- `PROCESS_THREAD(udp_client_process, ...)` registers a UDP socket with `simple_udp_register()` (local `UDP_CLIENT_PORT`, remote `UDP_SERVER_PORT`) and starts a periodic timer.
- Every ~60s (with jitter), if the RPL root is reachable (`node_is_reachable()` and `get_root_ipaddr()`), the client sends the current `temperature` to the DAG root via `simple_udp_sendto()`.
- `udp_rx_callback()` is triggered when the server replies; it just logs “Received high temperature alert!”.

## Server (udp_server.c)
- `udp_server_process` starts the node as RPL root (`root_start()`) and registers UDP reception on `UDP_SERVER_PORT`.
- `udp_rx_callback()` runs on every received reading: it logs the sender, stores new client IPv6 addresses in `receivers[]` (up to `MAX_RECEIVERS`), pushes the reading in a circular buffer `readings[]` (size `MAX_READINGS`), then computes the average of non-zero entries.
- If `average > ALERT_THRESHOLD`, it sets `alert=1` and wakes `udp_alert_process`.
- `udp_alert_process` waits for `alert==1`, then sends an “alert” to each known receiver, spacing transmissions with `ALERT_INTERVAL` random staggering to avoid back-to-back UDP sends. :contentReference[oaicite:1]{index=1}
