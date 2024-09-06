#!/usr/bin/env python3

import zmq
import struct
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
frontend.bind("tcp://*:5555")

while True:
    message = frontend.recv_multipart()
    print("message received")
    for part in message:
        if(len(part) != 1):
            l = int(len(part)/2)
            v = struct.unpack_from(f"{l}H", part)
            i = 0
            for b in v:
                print(f'{b:04x} ', end='')
                i = i + 1
                if(i % 8) == 0:
                    print("")
                    i = 0

