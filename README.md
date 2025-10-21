# Proyecto Kafka + Spark Streaming

Este proyecto demuestra el procesamiento de datos en tiempo real utilizando Apache Kafka y Apache Spark (PySpark).

## Archivos
- **kafka_producer.py**: envía datos simulados (sensor_id, temperatura, humedad, timestamp) al topic `sensor_data` de Kafka.
- **spark_streaming_consumer.py**: consume los datos en tiempo real desde Kafka usando Spark Streaming, y calcula promedios por minuto.

## Ejecución
1. Inicia Zookeeper y Kafka.
2. Crea el topic:
   ```bash
   bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092
