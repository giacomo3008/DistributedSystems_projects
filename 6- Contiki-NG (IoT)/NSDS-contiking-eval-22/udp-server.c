#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
#include "random.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678
#define MAX_RECEIVERS 10
#define MAX_READINGS 10

#define ALERT_INTERVAL (4 * CLOCK_SECOND)
#define ALERT_THRESHOLD 19

static struct simple_udp_connection udp_conn;
static uint8_t alert = 0; // Will be 0 if no alert, 1 otherwise

static uip_ipaddr_t receivers[MAX_RECEIVERS];
static uint8_t known_receivers = 0;

static unsigned readings[MAX_READINGS];
static unsigned next_reading = 0;

PROCESS(udp_server_process, "UDP server");
PROCESS(udp_alert_process, "Alert process");
AUTOSTART_PROCESSES(&udp_server_process, &udp_alert_process);
/*---------------------------------------------------------------------------*/
static void
udp_rx_callback(struct simple_udp_connection *c,
                const uip_ipaddr_t *sender_addr,
                uint16_t sender_port,
                const uip_ipaddr_t *receiver_addr,
                uint16_t receiver_port,
                const uint8_t *data,
                uint16_t datalen)
{
  unsigned reading = *(unsigned *)data;
  LOG_INFO("Received reading %u from ", reading);
  LOG_INFO_6ADDR(sender_addr);
  LOG_INFO_("\n");

  static uint8_t i = 0;
  static uint8_t found = 0;

  for (i = 0; i < known_receivers; i++)
  {
    LOG_INFO("Known ");
    LOG_INFO_6ADDR(&receivers[i]);
    LOG_INFO_("!\n");
  }

  for (i = 0; i < known_receivers; i++)
  {
    if (uip_ipaddr_cmp(&receivers[i], sender_addr))
    {
      LOG_INFO("Found ");
      LOG_INFO_6ADDR(sender_addr);
      LOG_INFO_("!\n");
      found = 1;
    }
  }
  if (!found)
  {
    LOG_INFO("Added ");
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("!\n");
    uip_ipaddr_copy(&receivers[known_receivers], sender_addr);
    known_receivers++;
  }

  /* Add reading */
  readings[next_reading++] = reading;
  if (next_reading == MAX_READINGS)
  {
    next_reading = 0;
  }

  /* Compute average */
  float average;
  unsigned sum = 0;
  unsigned no = 0;
  for (i = 0; i < MAX_READINGS; i++)
  {
    if (readings[i] != 0)
    {
      sum = sum + readings[i];
      no++;
    }
  }
  average = ((float)sum) / no;
  LOG_INFO("Current average is %f \n", average);

  if (average > ALERT_THRESHOLD)
  {
    LOG_INFO("Setting alert! \n");
    alert = 1;
    process_poll(&udp_alert_process);
  }
  else
  {
    LOG_INFO("Not setting alert... \n");
    alert = 0;
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_alert_process, ev, data)
{
  static struct etimer stagger_timer;
  static uint8_t i = 0;

  PROCESS_BEGIN();

  while (1)
  {

    PROCESS_WAIT_EVENT_UNTIL(alert == 1);

    LOG_INFO("Sending alerts out\n");
    for (i = 0; i < known_receivers; i++)
    {

      etimer_set(&stagger_timer, random_rand() % ALERT_INTERVAL);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&stagger_timer));

      LOG_INFO("Sending alert to ");
      LOG_INFO_6ADDR(&receivers[i]);
      LOG_INFO_("\n");
      (&udp_conn, &alert, sizeof(alert), &receivers[i]);
    }
  }
  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();

  static uint8_t i = 0;
  /* Initialize temperature buffer */
  for (i = 0; i < next_reading; i++)
  {
    readings[i] = 0;
  }

  /* Initialize DAG root */
  NETSTACK_ROUTING.root_start();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
