import paho.mqtt.client as mqtt
import json
import time
from loguru import logger
import os
from dotenv import load_dotenv
from pydantic import ValidationError


from .init import init

def mySenderLog(func):
  def wrapper():
    logger.info("--------------------------")
    func()
    # Wait for engine to receive
    logger.info("waits for 5s")
    time.sleep(5)
    logger.info("done waiting") 
    logger.info("--------------------------\n\n\n\n\n\n")

  return wrapper


# Get the environment variables, which can be self-specified or assignment-specified
InputTopicPrefix, OutputTopicPrefix, MQTTHost, MQTTPort = init(logfilePath="log/webapp.log")

# Mock data, for 4 cases: not eligible, single, couple no child, couple with children
def publish_message(client):

  basePayload = {
      "numberOfChildren": 0,  
      "familyComposition": "single",  
      "familyUnitInPayForDecember": True 
  }

  cases = [
    # Case 1: not eligible
    {
      **basePayload,
      "id": "not-eligible",
      "familyUnitInPayForDecember": False 
    },
    # Case 2: Single with no children
    {
      **basePayload,
      "id": "single-no-children",
    },
    # Case 3: Couple no children
    {
      **basePayload,
      "id": "copule-no-children",
      "familyComposition": "couple",  
    },
    # Case 4: Couple with children
    {
      **basePayload,
      "id": "couple-with-children",
      "numberOfChildren": 2,  
      "familyComposition": "couple",  
    },
    # Case 5: Single with children
    {
      **basePayload,
      "id": "single-with-children",
      "numberOfChildren": 3,  
    },
    # Case 6: Missing fields in input data 
    {
      "id": "missing-fields",
      "numberOfChildren": 3,  
    },
    # Case 7: Invalid input value
    {
      **basePayload,
      "id": "invalid-input-value",
      "numberOfChildren": 3,
      "familyComposition": "Christmas",
    },
  ]

  for c in cases: 
    @mySenderLog
    def publishAndLog():
      topic = f"{InputTopicPrefix}topic-{c['id']}"
      client.publish(topic, json.dumps(c))
      logger.info(f"\nBroker: Published to {topic}:\n{json.dumps(c)}\n")
    publishAndLog()




  # Special Case 1: Invalid topic ID
  @mySenderLog
  def sp1():
    client.publish(InputTopicPrefix, None)
    logger.info(f"\nBroker: Published to {InputTopicPrefix}:\nNone\n")
  sp1() 
  
  # Special Case 2: Invalid payload
  @mySenderLog
  def sp2():
    payload_invalid = "123" 
    topic = InputTopicPrefix + "topic-invalid-payload" 
    client.publish(topic, json.dumps(payload_invalid))
    logger.info(f"\nBroker: Published to {topic}:\n{json.dumps(payload_invalid)}")  
  sp2()
  

def on_connect(client, userdata, flags, rc, properties):
  logger.info(f"Broker connected: result code {rc}\n\n\n")
  client.subscribe(OutputTopicPrefix + "#")
  
  publish_message(client)

def on_message(client, userdata, msg):
  logger.info(f"\n\nReceived from Broker: {msg.topic}\n{msg.payload}\n\n")

def main():
  mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
  mqttc.on_connect = on_connect
  mqttc.on_message = on_message
  mqttc.connect(MQTTHost, int(MQTTPort), 60)
  mqttc.loop_forever()
  
if __name__=="__main__":
  main()
