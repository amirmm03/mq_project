import json

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_library', exchange_type='direct')

is_reserve = input("for reserve enter 1, for adding book enter 2 : ")
is_reserve = int(is_reserve) == 1

book_name = input("enter book name : ")

if is_reserve:
    channel.basic_publish(exchange='direct_library', routing_key="reserve",
                          body=bytes(json.dumps({"name": book_name}), 'ascii'))
else:
    channel.basic_publish(exchange='direct_library', routing_key="add",
                          body=bytes(json.dumps({"name": book_name}), 'ascii'))

print("sent message")
connection.close()
