import producer_server
from pathlib import Path
import logging
from sys import exit

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run_kafka_server():
	# Get the json file path
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"
    
    # Instantiate Kafka Producer
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="pd.calls",
        bootstrap_servers="localhost:9092",
        client_id="pd.calls.producer",
        acks='all',
#        compression_type='lz4',
        linger_ms=1000
    )
    return producer


def feed():
    try:
        producer = run_kafka_server()
        logger.info("Successfully started Kafka Producer")
    except Exception as e:
        logger.warning("Error starting Kafka Producer")
        logger.error(f"{e}")
        exit()
    
    producer.generate_data()


if __name__ == "__main__":
    try:
        feed()
    except KeyboardInterrupt:
        print("Shutting Down")
