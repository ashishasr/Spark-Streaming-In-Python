#%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices
#ForMultipleQueryDemo.py
#$KAFKA_HOME/bin/kafka-topics.sh --create --topic notifications --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

#ForKafkaAvroSinkDemo
$KAFKA_HOME/bin/kafka-topics.sh --create --topic customer-rewards --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
