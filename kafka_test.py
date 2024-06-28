from kafka import KafkaConsumer, KafkaProducer
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, filename='app_kfk.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')
producer = KafkaProducer(bootstrap_servers='172.25.98.94:29092')
consumer = KafkaConsumer('test', bootstrap_servers='172.25.98.94:49092')

producer.send('Jim_Topic', b'Message from PyCharm')
producer.send('Jim_Topic', key=b'message-two', value=b'This is Kafka-Python')
producer.flush()
producer.close()
for message in consumer:
    print(message.value)
    logging.warning(f"Received message: {message.value}")
    consumer.close()