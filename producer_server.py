from kafka import KafkaProducer
import json
import time
import logging

logger = logging.getLogger(__name__)

class ProducerServer(KafkaProducer):
    
    namespace = "san.francisco.crime.stats"

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = f"{ProducerServer.namespace}.{topic}"

    # Generating dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            for line in json.load(f):
                message = self.dict_to_binary(line)
                try:
                    self.send(self.topic, value=message)
                except Exception as e:
                    logger.warning(f"{e}")
                time.sleep(1)

    # Return the json dictionary after converting to binary
    def dict_to_binary(self, json_dict):
        message = bytes(json.dumps(json_dict).encode('utf-8'))
        return message