from kafka import KafkaConsumer
import asyncio
from sys import exit
import logging
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

async def consumer(topic):
    try:
        consumer = KafkaConsumer(bootstrap_servers = "localhost:9092"
                               , group_id = "SF_Crime_Analysis")
        logger.info("Successfully created Kafka Consumer")
    except Exception as e:
        logger.warning("Error creating Kafka Consumer")
        logger.error(f"{e}")
        exit()

    consumer.subscribe([topic])
    while True:
        records_dict = consumer.poll(1.0)        #Poll returns Dictionary
        if records_dict is None:
            logger.info("no message received by consumer")
        else:
            records_list = records_dict.values() #Values contians List of records
            for records in records_list:
                for record in records:
                    print(f"consumed message {json.loads(record.value)}")
        await asyncio.sleep(2.5)
    
def main():
    topic = "san.francisco.crime.stats.pd.calls"
    asyncio.run(consumer(topic))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down due to keyboard interruption...")