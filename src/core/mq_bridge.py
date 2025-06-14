import pika
import json
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# This will be the bridge between the message queue and the core

class MQBridge:
    def __init__(self, connection_params: Optional[pika.ConnectionParameters] = None):
        self.connection_params = connection_params or pika.ConnectionParameters('localhost')
        self.connection = None
        self._callbacks = {}

    def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def disconnect(self):
        """Close connection to RabbitMQ"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    def declare_queue(self, queue_name: str, durable: bool = True):
        """Declare a queue"""
        if not self.channel:
            self.connect()
        self.channel.queue_declare(queue=queue_name, durable=durable)

    def send_message(self, queue_name: str, message: dict):
        """Send a message to the specified queue"""
        if not self.channel:
            self.connect()

        try:
            self.declare_queue(queue_name)
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
            )
            logger.debug(f"Message sent to queue {queue_name}: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to {queue_name}: {e}")
            raise

    def subscribe(self, queue_name: str, callback: Callable):
        """Subscribe to a queue with a callback function"""
        if not self.channel:
            self.connect()

        self.declare_queue(queue_name)
        self._callbacks[queue_name] = callback

        def wrapper(ch, method, properties, body):
            try:
                message = json.loads(body.decode('utf-8'))
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error processing message from {queue_name}: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(queue=queue_name, on_message_callback=wrapper)
        logger.info(f"Subscribed to queue: {queue_name}")

    def start_consuming(self):
        """Start consuming messages (blocking call)"""
        if not self.channel:
            raise RuntimeError("No channel available. Call connect() first.")

        logger.info("Starting to consume messages...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumption...")
            self.channel.stop_consuming()

    def stop_consuming(self):
        """Stop consuming messages"""
        if self.channel:
            self.channel.stop_consuming()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
