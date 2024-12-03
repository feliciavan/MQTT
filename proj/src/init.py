from dotenv import load_dotenv
from loguru import logger
import os

def init(logfilePath="log/engine.log"):
  """
  Initializes the environment for the application.

  This function:
  1. Sets up logging by specifying the location of the log file ("log/engine.log").
  2. Loads environment variables from a `.env` file, if available, using `load_dotenv()`.
  3. Retrieves and returns the values of four environment variables: 
      - "InputTopicPrefix" 
      - "OutputTopicPrefix"
      - "MQTTHost"
      - "MQTTPort"
  
  The environment variables are expected to be used for configuring input/output topics, MQTT host, and port for the application.

  Returns:
      list: A list containing the values of the environment variables in the order:
            ["InputTopicPrefix", "OutputTopicPrefix", "MQTTHost", "MQTTPort"].
            If an environment variable is not set, `os.getenv()` will return `None`.
  """

  # Log file position
  logger.add(logfilePath)

  load_dotenv()
  return [
    os.getenv(ev)
    for ev in ["InputTopicPrefix", "OutputTopicPrefix", "MQTTHost", "MQTTPort"]
  ] 