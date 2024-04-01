import pika
import sys
import os
from send import email

def main():
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq", heartbeat=0))
        channel = connection.channel()

        # Declare the MP3 queue
        queue_name = os.environ.get("MP3_QUEUE")
        channel.queue_declare(queue=queue_name)

        def callback(ch, method, properties, body):
            # Call email notification function
            err = email.notification(body)
            if err:
                ch.basic_nack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        # Start consuming messages from the queue
        channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print("Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        channel.start_consuming()

    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
