import paho.mqtt.client as mqtt
import json
from loguru import logger
from .schema import InputDataSchema
from .init import init
import re

class RuleEngine:

  def __init__(self):
    self.InputTopicPrefix, \
    self.OutputTopicPrefix, \
    self.MQTTHost, \
    self.MQTTPort = init()

  # extract topic ID and data from the coming msg
  def _parseInputMsg(self, msg):
    # extract topic ID
    match = re.search(rf'^{re.escape(self.InputTopicPrefix)}([^/]+)$', msg.topic)
    if not match:
      raise Exception(f"Invalid topic: {msg.topic}")
    topicID = match.group(1)
    
    # extract input data
    data = json.loads(msg.payload.decode())
    return topicID, InputDataSchema(**data)

  # generate output data based on input data
  def _generateOutputData(self, data):
    isEligible = data.familyUnitInPayForDecember
    familyComposition = data.familyComposition
    childrenAmt = data.numberOfChildren
    rslt={
      "id": data.id,
      "isEligible": isEligible,
      "childrenAmount": childrenAmt*20.0,
      "baseAmount": 0.0,
      "supplementAmount": 0.0,
    }

    if not isEligible:
      return rslt

    if familyComposition == "single" and childrenAmt==0:
      baseAmt = 60.0
    if (familyComposition == "single" and childrenAmt>0) or familyComposition == "couple":
      baseAmt = 120.0
    
    supplementAmt = baseAmt + childrenAmt*20

    return {
      **rslt,
      "baseAmount": baseAmt,
      "supplementAmount": supplementAmt,
    }


  def _onConnect(self, client, userdata, flags, rc, properties):
    if rc == 0:
      logger.info("Engine: Connected successfully")
      client.subscribe(self.InputTopicPrefix + "#")
    else:
      logger.error(
        f"Engine: Connected failed with reason code {rc}"
      )


  def _onMessage(self, client, userdata, msg):

    logger.info(f"Engine Received: {msg.topic} {str(msg.payload)}")

    try:  
      topicID, inputData = self._parseInputMsg(msg)
    except Exception as e:
      logger.error(e)
      logger.error(type(e))
      return


    outputTopic = f"{self.OutputTopicPrefix}{topicID}"

    outputData = self._generateOutputData(inputData)

    # Publish to MQTT broker
    client.publish(outputTopic, json.dumps(outputData))
    logger.info(f"\nEngine Published to {outputTopic}:\n {outputData}\n\n")


  def run(self):
    self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    self.client.on_connect = self._onConnect
    self.client.on_message = self._onMessage

    self.client.connect(
      self.MQTTHost,
      int(self.MQTTPort),
      60
    )

    self.client.loop_forever()

if __name__=="__main__":
  ruleEngine = RuleEngine()
  ruleEngine.run()

