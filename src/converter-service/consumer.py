import pika
import sys
import os
import time
from pymongo import MongoClient
import gridfs
from convert import to_mp3

def main():
    try:
        # Connect to MongoDB
        client = MongoClient(os.environ.get('MONGODB_URI'))
        db_videos = client.videos
        db_mp3s = client.mp3s
        # gridfs
        fs_videos = gridfs.GridFS(db_videos)
        fs_mp3s = gridfs.GridFS(db_mp3s)

        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', heartbeat=0)
        )
        channel = connection.channel()

        # Declare the queue
        queue_name = os.environ.get("VIDEO_QUEUE")
        channel.queue_declare(queue=queue_name)

        def callback(ch, method, properties, body):
            err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
            if err:
                ch.basic_nack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        # Start consuming messages from the queue
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback
        )

        print("Waiting for messages, to exit press CTRL+C")

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
