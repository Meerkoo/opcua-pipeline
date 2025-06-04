import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timezone

from asyncua import Client
from asyncua.ua.uaerrors import UaStatusCodeError
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("opcua_client.log"),
    ],
)
logger = logging.getLogger(__name__)


with open("./config.json") as file:
    config = json.load(file)


OPC_UA_SERVER = os.getenv("OPC_UA_SERVER")
POLLING_INTERVAL = os.getenv("POLLING_INTERVAL")
USE_SUBSCRIPTIONS = os.getenv("USE_SUBSCRIPTIONS")
SUBSCRIPTION_PERIOD = int(os.getenv("SUBSCRIPTION_PERIOD"))

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

RETRY_DELAY = 1
MAX_RETRY_DELAY = 60

# Machine metadata - for tagging the time series data
MACHINE_METADATA = {
    "machine_id": config["machine_data"]["machine_id"],
    "location": config["machine_data"]["location"],
}

MONITORING_NODES = config["machine_data"]["data_points"]

shutdown_requested = False


class OpcUaClient:
    """Client for collecting data from OPC UA server and writing to InfluxDB"""

    def __init__(self, server_url, nodes_to_monitor):
        """Initialize the OPC UA client

        Args:
            server_url: URL of the OPC UA server
            nodes_to_monitor: List of node dictionaries with id, name, and unit
        """
        self.server_url = server_url
        self.nodes_to_monitor = nodes_to_monitor
        self.client = None
        self.connected = False
        self.subscription = None
        self.monitored_items = []

        # Initialize InfluxDB client
        self.influx_client = InfluxDBClient(
            url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG
        )

        # Create write API with batching options
        self.write_api = self.influx_client.write_api(
            write_options=WriteOptions(
                batch_size=50,
                flush_interval=100,
                jitter_interval=2_000,
                retry_interval=5_000,
                max_retries=5,
                max_retry_delay=30_000,
                exponential_base=2,
            )
        )

    async def connect(self):
        """Connect to the OPC UA server"""
        retry_delay = RETRY_DELAY
        max_retry_delay = MAX_RETRY_DELAY

        while not shutdown_requested:
            try:
                logger.info(f"Connecting to OPC UA server at {self.server_url}")

                self.client = Client(url=self.server_url, timeout=100)
                await self.client.connect()
                self.connected = True

                logger.info("Successfully connected to OPC UA server")
                return True

            except Exception as e:
                logger.error(f"Failed to connect to OPC UA server: {e}")

                # Exponential backoff for retries
                logger.info(f"Retrying connection in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)

                # Exponential backoff with jitter
                retry_delay = min(max_retry_delay, retry_delay * 2)
                retry_delay = retry_delay * (0.8 + 0.4 * random.random())
        return False

    async def disconnect(self):
        """Gracefully disconnect from the OPC UA server."""
        if not (self.client and self.connected):
            return

        try:
            if self.subscription:
                await self.subscription.delete()
                self.subscription = None
                self.monitored_items = []

            await self.client.disconnect()
            logger.info("Disconnected from OPC UA server")

        except Exception as e:
            logger.exception(f"Error during disconnect: {e}")

        finally:
            self.connected = False
            self.client = None

    async def setup_subscription(self):
        """Set up subscription for data changes."""
        if not self.connected:
            logger.error("Cannot set up subscription - not connected")
            return False

        try:
            self.subscription = await self.client.create_subscription(
                period=500, handler=SubHandler(self)
            )

            for name, info in self.nodes_to_monitor.items():
                node_id = info.get("node_id")
                if not node_id:
                    logger.warning(f"Skipping node {name} - no node_id specified")
                    continue

                logger.info(f"Subscribing to node '{name}' with node_id '{node_id}'")
                node = self.client.get_node(node_id)
                monitored_item = await self.subscription.subscribe_data_change(node)
                self.monitored_items.append(monitored_item)

            logger.info(
                f"Subscription successfully set up for {len(self.monitored_items)} nodes"
            )
            return True

        except Exception as e:
            logger.exception("Failed to set up subscription")
            return False

    async def read_nodes(self):
        """Read all configured nodes and return their values"""
        if not self.connected:
            logger.error("Cannot read nodes - not connected")
            return []

        results = []
        for node_info in self.nodes_to_monitor:
            try:
                node = self.client.get_node(node_info["id"])
                value = await node.read_value()

                data_point = {
                    "name": node_info,
                    "value": value,
                    "timestamp": datetime.now(timezone.utc),
                }

                results.append(data_point)

            except UaStatusCodeError as ua_error:
                logger.error(f"OPC UA error reading {node_info['name']}: {ua_error}")
            except Exception as e:
                logger.error(f"Error reading node {node_info['name']}: {e}")

        return results

    def write_to_influxdb(self, data_points):
        """Write collected data points to InfluxDB"""
        if not data_points:
            return

        try:
            points = []
            for point_data in data_points:
                value = point_data["value"]

                if value is not None:
                    point = (
                        Point("machine_data")
                        .tag("machine_id", MACHINE_METADATA["machine_id"])
                        .tag("location", MACHINE_METADATA["location"])
                        .tag("data_point", point_data["name"])
                        .time(point_data["timestamp"])
                    )

                    if isinstance(value, (int, float)):
                        point.field("value_numeric", float(value))
                    elif isinstance(value, str):
                        point.field("value_text", value)
                    elif isinstance(value, bool):
                        point.field("value_bool", value)
                    else:
                        raise ValueError(f"Unsupported data type: {type(value)}")

                    points.append(point)

            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")

    def process_subscription_data(self, node_id, value, timestamp):
        """Process data received from subscription"""
        # Find the corresponding node_info dictionary
        node = next(
            (
                node
                for node in self.nodes_to_monitor
                if self.nodes_to_monitor[node]["node_id"] == node_id.nodeid.to_string()
            ),
            None,
        )

        if node:
            data_point = {
                "name": node,
                "value": value,
                "timestamp": datetime.now(timezone.utc),
            }
            self.write_to_influxdb([data_point])
        else:
            logger.warning(f"Received data for unknown node ID: {node_id}")

    async def run_polling_loop(self):
        """Main polling loop to read data periodically"""
        logger.info(f"Starting polling loop with interval {POLLING_INTERVAL} seconds")

        while not shutdown_requested:
            try:
                if not self.connected:
                    success = await self.connect()
                    if not success:
                        await asyncio.sleep(5)
                        continue

                data_points = await self.read_nodes()

                # Write to InfluxDB if we have data
                if data_points:
                    self.write_to_influxdb(data_points)

                await asyncio.sleep(POLLING_INTERVAL)

            except asyncio.CancelledError:
                logger.info("Polling loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await self.disconnect()
                await asyncio.sleep(5)

    async def run_subscription_loop(self):
        """Run using the subscription model"""
        logger.info("Starting subscription-based data collection")

        while not shutdown_requested:
            try:
                if not self.connected:
                    success = await self.connect()
                    if not success:
                        await asyncio.sleep(5)
                        continue

                # Set up subscription
                if not self.subscription:
                    success = await self.setup_subscription()
                    if not success:
                        await self.disconnect()
                        await asyncio.sleep(5)
                        continue

                # Keep the loop alive while subscriptions are active
                while self.connected and not shutdown_requested:
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("Subscription loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in subscription loop: {e}")
                await self.disconnect()
                await asyncio.sleep(5)

    async def run(self):
        """Run the client using either polling or subscription model"""
        try:
            if USE_SUBSCRIPTIONS:
                await self.run_subscription_loop()
            else:
                await self.run_polling_loop()
        finally:
            await self.disconnect()
            if self.influx_client:
                self.write_api.close()
                self.influx_client.close()
                logger.info("InfluxDB client closed")


class SubHandler:
    """Subscription handler for OPC UA data change notifications"""

    def __init__(self, client):
        self.client = client

    def datachange_notification(self, node, val, data):
        """Callback for data changes"""
        try:
            timestamp = datetime.now()
            self.client.process_subscription_data(node, val, timestamp)
        except Exception as e:
            logger.error(f"Error in datachange_notification: {e}")

    def event_notification(self, event):
        """Callback for events"""
        logger.info(f"Received event: {event}")


async def shutdown(client):
    """Graceful shutdown"""
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested, closing connections...")

    if client:
        await client.disconnect()


def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, initiating shutdown...")
    global shutdown_requested
    shutdown_requested = True


async def main():
    """Main function"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 50)
    logger.info("OPC UA Data Collection Pipeline")
    logger.info(f"Server: {OPC_UA_SERVER}")
    logger.info(f"InfluxDB: {INFLUX_URL}")
    logger.info(
        f"Collection mode: {'Subscription' if USE_SUBSCRIPTIONS else 'Polling'}"
    )
    logger.info(f"Machine ID: {MACHINE_METADATA['machine_id']}")
    logger.info("=" * 50)

    client = OpcUaClient(OPC_UA_SERVER, MONITORING_NODES)

    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        await shutdown(client)


if __name__ == "__main__":
    asyncio.run(main())
