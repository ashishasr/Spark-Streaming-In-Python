#%KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic invoices
$KAFKA_HOME/bin/kafka-console-producer.sh --topic stock-trades --bootstrap-server localhost:9092