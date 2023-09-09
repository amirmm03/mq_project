import csv
import json
import os

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_library', exchange_type='direct')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

is_consumer_for_reserve = int(input("if consumer is for reserve enter 1. for adding enter 2 : ")) == 1

if is_consumer_for_reserve:
    channel.queue_bind(
        exchange='direct_library', queue=queue_name, routing_key="reserve")
else:
    channel.queue_bind(
        exchange='direct_library', queue=queue_name, routing_key="add")

name = "reserve" if is_consumer_for_reserve else "add"
print(f'Waiting for {name} tasks.')


def callback(ch, method, properties, body):
    print(f"received : {body}")
    text = (body.decode('utf8'))
    data = json.loads(text)

    base_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{base_dir}/{name}.csv"
    if not os.path.isfile(file_path):
        with open(name + ".csv", 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['name'])

    with open(name + ".csv", 'a') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([data['name']])

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback)

channel.start_consuming()
