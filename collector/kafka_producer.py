import json
from aiokafka import AIOKafkaProducer
from config import settings

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        await _producer.start()
    return _producer


async def publish(topic: str, value: dict, key: str | None = None) -> None:
    producer = await get_producer()
    await producer.send_and_wait(topic, value=value, key=key)


async def close_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None
