#!/usr/bin/env python3

import time, sys
import json
import logging

import subprocess as sp

import paho.mqtt.client as mqttClient

import paho.mqtt.subscribe as subscribe
import paho.mqtt.publish as pub

from pms_a003 import Sensor

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
#         # alternatively use a single call to client.subscribe with a list of all the topics
#         # see also https://stackoverflow.com/questions/46191152/subscribe-and-read-several-topics-on-mqtt-mosquitto-using-python-paho
#         client.subscribe("home/bedroom/bed_warm/set")
#         client.subscribe("home/salon/jalousie_up/set")
#         client.subscribe("home/salon/light_alt/set")
    else:
        print("Connection failed")

def on_message(client, userdata, message):

    now = time.time()
    print( f"{now}> Message received on { message.topic}: {int(message.payload)} {type(int(message.payload))}" )
    sys.stdout.flush()

    # take corresponding action:
    if 'light_alt' in message.topic: setLivRoomLight( int(message.payload) )
    if 'bed_warm'  in message.topic: setBed( int(message.payload) )

    # report change done
    mqtt_topic = message.topic.replace('/set', '/get')
    msg = -1
    if int(message.payload) == 0:
        msg = 0
    else:
        msg = 1
    print( f'{now}> reporting  done  to {mqtt_topic} - {msg} ' )
    pub.single( mqtt_topic, msg, hostname="rpi4b-1.local")

    
def main():
    Connected = False

    broker_address= "rpi4b-1.local"
    port = 1883

    client = mqttClient.Client("Python")
    client.connect(broker_address, port=port)
    client.loop_start()

    air_mon = Sensor()

    try:
        while True:

            values = None
            try:
                air_mon.connect_hat(port="/dev/ttyS0", baudrate=9600)
                values = air_mon.read()
                air_mon.disconnect_hat()
            except Exception as e:
                logging.info( f'ERROR 1 from sensor: got: {type(e)} - {str(e)}' )
                if 'could not open port /dev/ttyS0: [Errno 13] Permission denied' in str(e):
                    ret = sp.call( 'sudo chmod go+rw /dev/ttyS0', shell=True )
                    logging.info( f'resetting perms on ttyS0 returned: {ret}' )
                logging.info( f'retrying in 30 sec ... ' )
                
                time.sleep(30) # wait a bit ... 
                try:
                    air_mon.connect_hat(port="/dev/ttyS0", baudrate=9600)
                    values = air_mon.read()
                    air_mon.disconnect_hat()
                    logging.info( '... retry successful' )
                except Exception as e:
                    logging.info( f'ERROR 2 from sensor: got: {type(e)} - {str(e)}' )
                    logging.info( f'giving up ... ' )
                    values = None

            if values:
                info = { 'PMS 1' : values.pm10_cf1, 'PMS 2.5' : values.pm25_cf1, 'PMS 10' : values.pm100_cf1, 'timestamp': time.time() }
                topic = '/rpisensors/rpi3-5/airquality'
                # logging.info( f'going to publish {info} to {topic}' )
                (result,mid) = client.publish(topic=topic, payload=json.dumps(info) )
                if result != 0:
                    logging.info( f'publishing returned error: {result}, {mid}' )
                    if result == 4:
                        logging.info( 'found error 4 -- reconnecting and retrying ' )
                        client.disconnect()
                        client.loop_stop()
                        client.connect(broker_address, port=port)
                        client.loop_start()
                        (result,mid) = client.publish(topic=topic, payload=json.dumps(info) )
                        if result != 0:
                            logging.info( f're-publishing returned error: {result}, {mid}' )
                    
                # got some data, so we sleep a bit longer
                time.sleep(300) # sleep 5 min
            else:
                # got no data from sensor, try again in 30 sec ... 
                time.sleep(30)

    except KeyboardInterrupt:
        print ("exiting")
        client.disconnect()
        client.loop_stop()

if __name__ == '__main__':

    logging.basicConfig( format = '[%(asctime)s] %(levelname)s: %(message)s', level=logging.INFO )
    main()
