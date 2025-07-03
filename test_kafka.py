import time
from kafka import KafkaProducer, KafkaConsumer
from json import dumps

# Настройки
bootstrap_servers = ['localhost:9092']  # Оставь как есть для локального Kafka
topic = 'test_topic'

# Производитель
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Отправка тестовых сообщений
for i in range(10):
    data = {'message': f'Test {i}', 'timestamp': time.time()}
    producer.send(topic, value=data)
    print(f"Отправлено: {data}")
producer.flush()

# Потребитель
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Чтение сообщений
print("Получено:")
for message in consumer:
    data = eval(message.value)  # Парсинг JSON
    print(data)
    if 'Test 9' in data['message']:
        break

consumer.close()
producer.close()