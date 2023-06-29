import os
import sys
import time

import gridfs
import pika
from convert import to_mp3
from pymongo import MongoClient
from src import settings


def main():
    client = MongoClient(settings.mongo_connection_str)
    db_videos = client.videos
    db_mp3s = client.mp3s
    # gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    # rabbitmq connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=settings.rabbitmq_host)
    )
    channel = connection.channel()

    def callback(ch, method, properties, body):
        err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=settings.video_queue,
        on_message_callback=callback,
    )

    print("Waiting for messages. To exit press CTRL+C")

    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
