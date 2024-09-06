#!/usr/bin/env python3

import zmq
from struct import unpack
from time import sleep

context = zmq.Context()

##

# run control socket

#socket = context.socket(zmq.PUB)
#socket.bind("tcp://0.0.0.0:4444")
#topic = "control"
#payload = "start"
#
#sleep(1)
#
#socket.send_string(topic, flags=zmq.SNDMORE)
#socket.send_string(payload)

##

frontend = context.socket(zmq.ROUTER)

poller = zmq.Poller()
poller.register(frontend, zmq.POLLIN)
frontend.bind("tcp://*:5555")

while True:
    message = frontend.recv_multipart()
    print("message received")
    #print(message[1][0:32])
