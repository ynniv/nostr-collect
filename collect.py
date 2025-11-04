#!/usr/bin/env python3
"""
Scalable Nostr Event Relay Bridge
Optimized for handling 300+ source relays with reliable single-event processing.
"""

import asyncio
import json
import logging
import signal
import time
import argparse
import random
import traceback
from pathlib import Path
from collections import OrderedDict, deque
from typing import Set, Dict, List, Optional, Tuple
from dataclasses import dataclass
from contextlib import asynccontextmanager
import websockets
import websockets.exceptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class RelayStats:
    """Statistics for a relay connection."""
    url: str
    connected: bool = False
    events_received: int = 0
    last_event_time: float = 0
    connection_attempts: int = 0
    last_error: Optional[str] = None
    error_count: int = 0


class LRUCache:
    """Thread-safe LRU cache implementation for event deduplication."""

    def __init__(self, max_size: int = 50000):
        self.max_size = max_size
        self.cache: OrderedDict[str, float] = OrderedDict()
        self._lock = asyncio.Lock()

    async def contains(self, event_id: str) -> bool:
        """Check if event ID exists in cache and update its position."""
        async with self._lock:
            if event_id in self.cache:
                self.cache.move_to_end(event_id)
                return True
            return False

    async def add(self, event_id: str) -> None:
        """Add event ID to cache."""
        async with self._lock:
            if event_id in self.cache:
                self.cache.move_to_end(event_id)
            else:
                self.cache[event_id] = time.time()
                if len(self.cache) > self.max_size:
                    cleanup_count = max(1, self.max_size // 10)
                    for _ in range(cleanup_count):
                        self.cache.popitem(last=False)

    def size(self) -> int:
        return len(self.cache)


class ConnectionPool:
    """Manages relay connections with health monitoring."""

    def __init__(self, max_concurrent: int = 200, max_per_batch: int = 20):
        self.max_concurrent = max_concurrent
        self.max_per_batch = max_per_batch
        self.active_connections = {}
        self.connection_semaphore = asyncio.Semaphore(max_concurrent)
        self.relay_stats = {}
        self.failed_relays = set()
        self.health_monitor_running = False

    async def start_health_monitor(self):
        if not self.health_monitor_running:
            self.health_monitor_running = True
            asyncio.create_task(self._health_monitor_loop())

    async def _health_monitor_loop(self):
        try:
            while self.health_monitor_running:
                await asyncio.sleep(300)  # Check every 5 minutes
                current_time = time.time()

                # Clean up inactive relays
                inactive_relays = []
                for relay_url, stats in self.relay_stats.items():
                    if (not stats.connected and
                        current_time - stats.last_event_time > 3600):  # 1 hour
                        inactive_relays.append(relay_url)

                for relay_url in inactive_relays:
                    if relay_url in self.failed_relays:
                        self.failed_relays.remove(relay_url)

        except asyncio.CancelledError:
            logger.debug("Health monitor cancelled")
        finally:
            self.health_monitor_running = False

    @asynccontextmanager
    async def get_connection(self, relay_url: str):
        async with self.connection_semaphore:
            try:
                websocket = await self._connect_relay(relay_url)
                if websocket:
                    self.active_connections[relay_url] = websocket
                    yield websocket
                else:
                    yield None
            finally:
                if relay_url in self.active_connections:
                    try:
                        await self.active_connections[relay_url].close()
                    except:
                        pass
                    del self.active_connections[relay_url]

    async def _connect_relay(self, relay_url: str) -> Optional[websockets.WebSocketServerProtocol]:
        stats = self.relay_stats.setdefault(relay_url, RelayStats(relay_url))
        stats.connection_attempts += 1

        try:
            websocket = await asyncio.wait_for(
                websockets.connect(
                    relay_url,
                    ping_interval=30,
                    ping_timeout=15,
                    close_timeout=5,
                    max_size=None,
                    compression=None,
                    max_queue=32
                ),
                timeout=5
            )

            stats.connected = True
            stats.last_error = None
            if relay_url in self.failed_relays:
                self.failed_relays.remove(relay_url)

            return websocket

        except asyncio.TimeoutError:
            stats.connected = False
            stats.last_error = "Connection timeout"
            stats.error_count += 1
        except Exception as e:
            stats.connected = False
            stats.last_error = str(e)
            stats.error_count += 1

            if stats.error_count > 3:
                self.failed_relays.add(relay_url)

        return None

    def get_healthy_relays(self, relay_list: List[str]) -> List[str]:
        return [r for r in relay_list if r not in self.failed_relays]

    def get_stats(self) -> Dict:
        total_events = sum(stats.events_received for stats in self.relay_stats.values())
        connected_count = sum(1 for stats in self.relay_stats.values() if stats.connected)
        failed_count = len(self.failed_relays)

        return {
            'active_connections': len(self.active_connections),
            'total_relays': len(self.relay_stats),
            'connected': connected_count,
            'failed': failed_count,
            'total_events_received': total_events
        }

    async def fast_connect_test(self, relay_urls: List[str], max_concurrent: int = 50) -> Tuple[List[str], List[str]]:
        logger.info(f"Testing connections to {len(relay_urls)} relays...")

        working_relays = []
        failed_relays = []

        async def test_single_relay(relay_url: str) -> bool:
            try:
                websocket = await asyncio.wait_for(
                    websockets.connect(relay_url, close_timeout=2),
                    timeout=3
                )
                await websocket.close()
                return True
            except:
                return False

        semaphore = asyncio.Semaphore(max_concurrent)

        async def test_with_semaphore(relay_url: str):
            async with semaphore:
                is_working = await test_single_relay(relay_url)
                if is_working:
                    working_relays.append(relay_url)
                else:
                    failed_relays.append(relay_url)

        start_time = time.time()
        await asyncio.gather(*[test_with_semaphore(url) for url in relay_urls], return_exceptions=True)
        test_duration = time.time() - start_time

        logger.info(f"Connection test completed in {test_duration:.1f}s: {len(working_relays)} working, {len(failed_relays)} failed")

        # Mark failed relays
        for relay_url in failed_relays:
            self.failed_relays.add(relay_url)
            stats = self.relay_stats.setdefault(relay_url, RelayStats(relay_url))
            stats.error_count = 5
            stats.last_error = "Failed connection test"

        return working_relays, failed_relays


class NostrRelayBridge:
    """Clean Nostr relay bridge with single-event processing."""

    def __init__(self, source_relays: List[str], destination_relay: str,
                 kinds: List[int], cache_size: int = 50000,
                 max_concurrent: int = 200, batch_size: int = 20):
        self.source_relays = source_relays
        self.destination_relay = destination_relay
        self.kinds = kinds
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size

        # Components
        self.cache = LRUCache(cache_size)
        self.connection_pool = ConnectionPool(max_concurrent, batch_size)
        self.event_queue = deque(maxlen=500000)
        self.queue_processing = False

        # Statistics with tracking for failures
        self.processed_count = 0
        self.forwarded_count = 0
        self.failed_count = 0
        self.queued_count = 0
        self.duplicate_count = 0
        self.running = False
        self.shutdown_initiated = False
        self.dest_websocket = None

    async def process_relay_batch(self, relay_batch: List[str]) -> None:
        logger.info(f"Starting batch of {len(relay_batch)} relays")

        tasks = []
        for relay_url in relay_batch:
            task = asyncio.create_task(self.process_single_relay(relay_url))
            tasks.append(task)

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in relay batch processing: {e}")

    async def process_single_relay(self, relay_url: str) -> None:
        stats = self.connection_pool.relay_stats.setdefault(relay_url, RelayStats(relay_url))
        consecutive_failures = 0

        if relay_url in self.connection_pool.failed_relays:
            consecutive_failures = 3

        while self.running:
            try:
                if relay_url in self.connection_pool.failed_relays and consecutive_failures > 0:
                    wait_time = min(1800, 60 * (2 ** min(consecutive_failures, 6)))
                    await asyncio.sleep(wait_time)
                    consecutive_failures += 1
                    continue

                async with self.connection_pool.get_connection(relay_url) as websocket:
                    if not websocket:
                        consecutive_failures += 1
                        wait_time = min(300, 30 * (2 ** min(consecutive_failures, 4)))
                        await asyncio.sleep(wait_time)
                        continue

                    if consecutive_failures > 0:
                        logger.info(f"Successfully connected to relay: {relay_url}")
                    consecutive_failures = 0

                    await self.subscribe_to_kinds(websocket, relay_url)

                    last_message_time = time.time()

                    async for message in websocket:
                        if not self.running:
                            break

                        last_message_time = time.time()

                        try:
                            data = json.loads(message)

                            if isinstance(data, list) and len(data) >= 3 and data[0] == "EVENT":
                                event = data[2]
                                await self.handle_event(event, relay_url)
                                stats.events_received += 1
                                stats.last_event_time = time.time()

                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON from {relay_url}")
                        except Exception as e:
                            logger.error(f"Error processing message from {relay_url}: {e}")
                            break

                    if time.time() - last_message_time > 600:
                        logger.debug(f"Relay {relay_url} idle for 10 minutes, reconnecting")

            except asyncio.CancelledError:
                break
            except websockets.exceptions.ConnectionClosed:
                consecutive_failures += 1
            except Exception as e:
                consecutive_failures += 1
                stats.error_count += 1

            if self.running and consecutive_failures > 0:
                base_wait = min(300, 30 * (2 ** min(consecutive_failures, 4)))
                jitter = random.uniform(0.8, 1.2)
                wait_time = base_wait * jitter
                await asyncio.sleep(wait_time)

    async def handle_event(self, event: Dict, source_relay: str) -> None:
        event_id = event.get('id')
        if not event_id:
            return

        self.processed_count += 1

        try:
            is_duplicate = await self.cache.contains(event_id)
            if not is_duplicate:
                await self.cache.add(event_id)

                # Queue the event
                if len(self.event_queue) >= self.event_queue.maxlen:
                    logger.error(f"PERMANENT FAILURE: Event queue full, dropping oldest event")
                    self.failed_count += 1

                self.event_queue.append(event)
                self.queued_count += 1

                # Ensure queue processor is running
                if not self.queue_processing:
                    logger.error("CRITICAL: Queue processor not running!")
                    asyncio.create_task(self.process_event_queue())
            else:
                self.duplicate_count += 1

        except Exception as e:
            logger.error(f"Error handling event {event_id}: {e}")
            self.failed_count += 1

    async def subscribe_to_kinds(self, websocket: websockets.WebSocketServerProtocol, relay_url: str) -> None:
        subscription_id = f"kinds_{int(time.time())}_{random.randint(1000, 9999)}"

        req_message = [
            "REQ",
            subscription_id,
            {
                "kinds": self.kinds,
                "limit": 50
            }
        ]

        try:
            await websocket.send(json.dumps(req_message))
        except Exception as e:
            logger.error(f"Failed to subscribe to {relay_url}: {e}")

    async def process_event_queue(self) -> None:
        """Process events one by one, only removing after successful send."""
        if self.queue_processing:
            return

        self.queue_processing = True
        logger.info("Started queue processing task")

        consecutive_empty_checks = 0

        try:
            while self.running:
                # Wait for destination connection
                if not (self.dest_websocket and
                       hasattr(self.dest_websocket, 'close_code') and
                       self.dest_websocket.close_code is None):
                    await asyncio.sleep(0.5)
                    continue

                if not self.event_queue:
                    consecutive_empty_checks += 1
                    if consecutive_empty_checks > 20:
                        await asyncio.sleep(1.0)
                    elif consecutive_empty_checks > 5:
                        await asyncio.sleep(0.5)
                    else:
                        await asyncio.sleep(0.1)
                    continue
                else:
                    consecutive_empty_checks = 0

                # Process one event at a time - peek first, don't remove yet
                event_to_send = self.event_queue[0]

                success = await self.send_single_event(event_to_send)

                if success:
                    # Only remove after successful send
                    self.event_queue.popleft()
                    self.forwarded_count += 1
                    await asyncio.sleep(0.005)  # Small delay when successful
                else:
                    # Leave event in queue for retry
                    await asyncio.sleep(0.5)  # Wait before retry

        except Exception as e:
            logger.error(f"Critical error in queue processing: {e}")
        finally:
            self.queue_processing = False
            logger.error(f"Queue processor stopped with {len(self.event_queue)} events remaining")

    async def send_single_event(self, event: Dict) -> bool:
        if not (self.dest_websocket and
               hasattr(self.dest_websocket, 'close_code') and
               self.dest_websocket.close_code is None):
            return False

        try:
            event_message = ["EVENT", event]
            await asyncio.wait_for(
                self.dest_websocket.send(json.dumps(event_message)),
                timeout=3
            )
            return True

        except asyncio.TimeoutError:
            return False
        except websockets.exceptions.ConnectionClosed:
            return False
        except websockets.exceptions.InvalidState:
            return False
        except Exception as e:
            return False

    async def maintain_destination_connection(self) -> None:
        connection_failures = 0

        try:
            while self.running:
                try:
                    logger.info(f"Connecting to destination relay: {self.destination_relay}")
                    self.dest_websocket = await asyncio.wait_for(
                        websockets.connect(
                            self.destination_relay,
                            ping_interval=None,
                            ping_timeout=None,
                            close_timeout=10,
                            max_size=None,
                            compression=None
                        ),
                        timeout=8
                    )

                    logger.info("Connected to destination relay")
                    connection_failures = 0

                    # Monitor connection health
                    connection_start_time = time.time()
                    last_health_check = time.time()

                    while self.running:
                        try:
                            await asyncio.sleep(2)
                            current_time = time.time()

                            if (hasattr(self.dest_websocket, 'close_code') and
                                self.dest_websocket.close_code is not None):
                                connection_duration = current_time - connection_start_time
                                logger.warning(f"Destination connection closed after {connection_duration:.1f}s")
                                break

                            # Health status every 30 seconds
                            if current_time - last_health_check > 30:
                                connection_duration = current_time - connection_start_time
                                logger.info(f"Destination connection healthy for {connection_duration:.1f}s")
                                last_health_check = current_time

                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            logger.warning(f"Error monitoring destination connection: {e}")
                            break

                except asyncio.TimeoutError:
                    logger.error("Timeout connecting to destination relay")
                    connection_failures += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Destination connection error: {e}")
                    connection_failures += 1
                finally:
                    if self.dest_websocket:
                        try:
                            await self.dest_websocket.close()
                        except:
                            pass
                        self.dest_websocket = None

                if self.running:
                    queue_size = len(self.event_queue)
                    wait_time = min(10, 2 * (2 ** min(connection_failures, 2)))
                    logger.info(f"Reconnecting to destination in {wait_time}s... ({queue_size} events queued)")
                    await asyncio.sleep(wait_time)

        except asyncio.CancelledError:
            logger.debug("Destination connection task cancelled")

    async def status_reporter(self) -> None:
        try:
            while self.running:
                await asyncio.sleep(15)

                if not self.running:
                    break

                pool_stats = self.connection_pool.get_stats()
                queue_size = len(self.event_queue)

                unique_events = self.processed_count - self.duplicate_count
                forward_rate = (self.forwarded_count / unique_events * 100) if unique_events > 0 else 0

                dest_status = "Connected" if (self.dest_websocket and
                             hasattr(self.dest_websocket, 'close_code') and
                             self.dest_websocket.close_code is None) else "Disconnected"

                logger.info(
                    f"Status - Relays: {pool_stats['connected']}/{pool_stats['total_relays']} "
                    f"(Failed: {pool_stats['failed']}) | "
                    f"Events - Processed: {self.processed_count}, "
                    f"Duplicates: {self.duplicate_count}, "
                    f"Unique: {unique_events}, "
                    f"Queued: {self.queued_count}, "
                    f"Forwarded: {self.forwarded_count} ({forward_rate:.1f}%), "
                    f"Failed: {self.failed_count}, "
                    f"Current Queue: {queue_size} | "
                    f"Dest: {dest_status} | "
                    f"QueueProc: {'Running' if self.queue_processing else 'Stopped'}"
                )

                if self.processed_count > 100 and forward_rate < 50:
                    logger.warning(f"Low forward rate ({forward_rate:.1f}%) - check destination connection stability")

        except asyncio.CancelledError:
            logger.debug("Status reporter cancelled")

    async def run(self) -> None:
        logger.info("Starting Nostr relay bridge...")
        self.running = True

        def signal_handler():
            if not self.shutdown_initiated:
                logger.info("Received interrupt signal. Shutting down...")
                self.running = False
                self.shutdown_initiated = True
                for task in asyncio.all_tasks():
                    if not task.done():
                        task.cancel()

        loop = asyncio.get_running_loop()
        for sig in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(sig, signal_handler)

        try:
            # Start destination connection first
            dest_task = asyncio.create_task(self.maintain_destination_connection())
            await asyncio.sleep(3)

            # Start queue processor
            queue_task = asyncio.create_task(self.process_event_queue())

            # Start health monitor and status reporter
            await self.connection_pool.start_health_monitor()
            status_task = asyncio.create_task(self.status_reporter())

            # Test connections in parallel
            logger.info("Performing fast connection test on all relays...")
            working_relays, failed_relays = await self.connection_pool.fast_connect_test(
                self.source_relays,
                max_concurrent=min(100, len(self.source_relays))
            )

            if not working_relays:
                logger.error("No working relays found!")
                return

            logger.info(f"Found {len(working_relays)} working relays, starting event processing...")

            # Process relays in batches
            batch_tasks = []
            for i in range(0, len(working_relays), self.batch_size):
                batch = working_relays[i:i + self.batch_size]
                task = asyncio.create_task(self.process_relay_batch(batch))
                batch_tasks.append(task)

                if i < len(working_relays) - self.batch_size:
                    await asyncio.sleep(0.5)

            # Also process failed relays for recovery
            if failed_relays:
                logger.info(f"Also monitoring {len(failed_relays)} failed relays for recovery")
                for i in range(0, len(failed_relays), self.batch_size):
                    batch = failed_relays[i:i + self.batch_size]
                    task = asyncio.create_task(self.process_relay_batch(batch))
                    batch_tasks.append(task)

            logger.info(f"All relay tasks started! Processing events from {len(working_relays)} relays...")

            # Wait for all tasks
            all_tasks = [dest_task, queue_task, status_task] + batch_tasks
            await asyncio.gather(*all_tasks, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Main task cancelled")
        finally:
            self.running = False
            pool_stats = self.connection_pool.get_stats()
            logger.info(
                f"Bridge stopped. Final stats - "
                f"Processed: {self.processed_count}, "
                f"Forwarded: {self.forwarded_count}, "
                f"Failed: {self.failed_count}, "
                f"Queued: {len(self.event_queue)}"
            )


async def main():
    parser = argparse.ArgumentParser(description='Nostr Event Relay Bridge')
    parser.add_argument('--source-relays', '-s', nargs='*', help='Source relay URLs')
    parser.add_argument('--destination', '-d', required=True, help='Destination relay URL')
    parser.add_argument('--kinds', '-k', type=int, nargs='+', default=[1], help='Event kinds')
    parser.add_argument('--max-concurrent', type=int, default=200, help='Max concurrent connections')
    parser.add_argument('--batch-size', type=int, default=20, help='Relay batch size')
    parser.add_argument('--cache-size', type=int, default=50000, help='LRU cache size')
    parser.add_argument('--relay-file', help='File containing relay URLs (one per line)')

    args = parser.parse_args()

    # Load relays
    source_relays = []

    if args.source_relays:
        source_relays.extend(args.source_relays)

    if args.relay_file:
        try:
            with open(args.relay_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if line.startswith('wss://') or line.startswith('ws://'):
                            source_relays.append(line)
                        else:
                            logger.warning(f"Skipping invalid relay URL on line {line_num}: {line}")
        except FileNotFoundError:
            logger.error(f"Relay file not found: {args.relay_file}")
            return 1
        except Exception as e:
            logger.error(f"Error reading relay file: {e}")
            return 1

    if not source_relays:
        logger.error("No source relays specified. Use --source-relays or --relay-file")
        return 1

    # Remove duplicates
    seen = set()
    source_relays = [r for r in source_relays if not (r in seen or seen.add(r))]

    print(f"Nostr Bridge - {len(source_relays)} relays")
    print(f"Kinds: {args.kinds}")
    print(f"Destination: {args.destination}")
    print("-" * 60)

    bridge = NostrRelayBridge(
        source_relays=source_relays,
        destination_relay=args.destination,
        kinds=args.kinds,
        max_concurrent=args.max_concurrent,
        batch_size=args.batch_size,
        cache_size=args.cache_size
    )

    try:
        await bridge.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.error(f"Bridge failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        print("\nBridge terminated.")
        exit(0)
