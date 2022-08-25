#%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices
$KAFKA_HOME/bin/kafka-topics.sh --create --topic stock-trades --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
