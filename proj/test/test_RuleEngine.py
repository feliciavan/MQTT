import json

import unittest
from unittest.mock import MagicMock, patch, Mock

from parameterized import parameterized

from pydantic import ValidationError

from src.RuleEngine import RuleEngine

class TestRuleEngine(unittest.TestCase):
  def setUp(self):
    self.ruleEngine = RuleEngine()
    self.ruleEngine.InputTopicPrefix="iPrefix/"
    self.ruleEngine.OutputTopicPrefix="oPrefix/"

  @patch("src.RuleEngine.logger")
  def test__onConnect_failed(self, logger_mk):
    #setup
    client_mk = Mock()
    rc = 1
    #action
    self.ruleEngine._onConnect(client_mk, Mock(), Mock(), rc, Mock())
    #assert
    logger_mk.info.assert_not_called()
    logger_mk.error.assert_called_once_with(f"Engine: Connected failed with reason code {rc}")
    client_mk.subscribe.assert_not_called()

  @patch("src.RuleEngine.logger")
  def test__onConnect_success(self, logger_mk):
    #setup
    client_mk = Mock()
    rc = 0
    #action
    self.ruleEngine._onConnect(client_mk, Mock(), Mock(), rc, Mock())
    #assert
    logger_mk.info.assert_called_once_with("Engine: Connected successfully")
    client_mk.subscribe.assert_called_once()
    logger_mk.error.assert_not_called()

  @parameterized.expand([
    [
      {
        "id": "not-eligible",
        "numberOfChildren": 0,
        "familyComposition": "single",
        "familyUnitInPayForDecember": False
      },{
        "id": "not-eligible",
        "isEligible": False,
        "childrenAmount": 0.0,
        "baseAmount": 0.0,
        "supplementAmount": 0.0
      }
    ],[
      {
        "id": "single-no-children",
        "numberOfChildren": 0,
        "familyComposition": "single",
        "familyUnitInPayForDecember": True
      },{
        "id": "single-no-children",
        "isEligible": True,
        "childrenAmount": 0.0,
        "baseAmount": 60.0,
        "supplementAmount": 60.0
      }
    ],[
      {
        "id": "couple-no-children",
        "numberOfChildren": 0,
        "familyComposition": "couple",
        "familyUnitInPayForDecember": True
      },{
        "id": "couple-no-children",
        "isEligible": True,
        "childrenAmount": 0.0,
        "baseAmount": 120.0,
        "supplementAmount": 120.0
      }
    ],[
      {
        "id": "couple-with-children",
        "numberOfChildren": 2,
        "familyComposition": "couple",
        "familyUnitInPayForDecember": True
      },{
        "id": "couple-with-children",
        "isEligible": True,
        "childrenAmount": 40.0,
        "baseAmount": 120.0,
        "supplementAmount": 160.0
      }
    ],[
      {
        "id": "single-with-children",
        "numberOfChildren": 3,
        "familyComposition": "single",
        "familyUnitInPayForDecember": True
      },{
        "id": "single-with-children",
        "isEligible": True,
        "childrenAmount": 60.0,
        "baseAmount": 120.0,
        "supplementAmount": 180.0
      }
    ]
  ])
  @patch("src.RuleEngine.logger")
  def test__onMesssage_withValidInput(self, inputData, expected_outputData, logger_mk):
    #setup
    topicID = "topicID"
    inTopic = self.ruleEngine.InputTopicPrefix+topicID
    outTopic = self.ruleEngine.OutputTopicPrefix+topicID

    msg_mk = Mock()
    msg_mk.topic = inTopic
    msg_mk.payload=json.dumps(inputData).encode('utf-8')


    client_mk = Mock()

    #action
    self.ruleEngine._onMessage(client_mk, Mock(), msg_mk)

    #assert
    logger_mk.error.assert_not_called()
    client_mk.publish.assert_called_once_with(outTopic, json.dumps(expected_outputData))
    logger_mk.info.assert_called_with(
      f"\nEngine Published to {outTopic}:\n {expected_outputData}\n\n"
    )

  @parameterized.expand([
    [
      "fDec type error1",
      ValidationError,
      {
        "id": "fDec type error1",
        "numberOfChildren": 0,
        "familyComposition": "single",
        "familyUnitInPayForDecember": "False"
      }
    ],[
      "fDec type error2",
      ValidationError,
      {
        "id": "fDec type error2",
        "numberOfChildren": 0,
        "familyComposition": "single",
        "familyUnitInPayForDecember": 1
      }
    ],[
      "fComp type error",
      ValidationError,
      {
        "id": "fComp type error",
        "numberOfChildren": 0,
        "familyComposition": "group",
        "familyUnitInPayForDecember": True
      }
    ],[
      "numOfC type error1",
      ValidationError,
      {
        "id": "numOfC type error1",
        "numberOfChildren": -1,
        "familyComposition": "single",
        "familyUnitInPayForDecember": True
      }
    ],[
      "numOfC type error2",
      ValidationError,
      {
        "id": "numOfC type error2",
        "numberOfChildren": "1",
        "familyComposition": "single",
        "familyUnitInPayForDecember": True
      }
    ],[
      "id type error",
      ValidationError,
      {
        "id": 123,
        "numberOfChildren": 0,
        "familyComposition": "single",
        "familyUnitInPayForDecember": True
      }
    ],[
      "missing field error0",
      ValidationError,
      {
        "numberOfChildren": 0,
        "familyComposition": "couple",
        "familyUnitInPayForDecember": True
      }
    ],[
      "missing field error1",
      ValidationError,
      {
        "id": "missing field error1",
        "familyComposition": "couple",
        "familyUnitInPayForDecember": True
      }
    ],[
      "missing field error2",
      ValidationError,
      {
        "id": "missing field error2",
        "numberOfChildren": 0,
        "familyUnitInPayForDecember": True
      }
    ],[
      "missing field error3",
      ValidationError,
      {
        "id": "missing field error3",
        "numberOfChildren": 0,
        "familyComposition": "couple",
      }
    ],[
      "empty inputData",
      ValidationError,
      {}
    ],[ # invalid topicID
      '',
      Exception,
      None
    ]
  ])
  @patch("src.RuleEngine.logger")
  def test__onMesssage_withInValidInput(self, topicID, errorClass, inputData, logger_mk):
    #setup
    inTopic = self.ruleEngine.InputTopicPrefix+topicID
    outTopic = self.ruleEngine.OutputTopicPrefix+topicID

    msg_mk = Mock()
    msg_mk.topic = inTopic
    msg_mk.payload=json.dumps(inputData).encode('utf-8')


    client_mk = Mock()

    #action
    self.ruleEngine._onMessage(client_mk, Mock(), msg_mk)

    #assert
    args, kwargs = logger_mk.error.call_args
    self.assertEqual(args[0], errorClass)
    client_mk.publish.assert_not_called()
