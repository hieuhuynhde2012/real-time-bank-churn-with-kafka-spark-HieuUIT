#processing_with_svm_sklearn
bin\windows\kafka-topics.bat --create --topic  producer_svm --bootstrap-server localhost:9092
bin\windows\kafka-topics.bat --create --topic  prediction_svm --bootstrap-server localhost:9092

#processing_with_sparkml
bin\windows\kafka-topics.bat --create --topic  producerrf_sparkml --bootstrap-server localhost:9092
bin\windows\kafka-topics.bat --create --topic  prediction_rf_sparkml --bootstrap-server localhost:9092
