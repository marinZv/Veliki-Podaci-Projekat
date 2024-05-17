package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class Consumer {

    private static final String intradayCsv = "intraday.csv";
    private static final String dailyCsv = "daily.csv";
    private static final String dailyAdjustedCsv = "daily-adjusted.csv";


    private static final Logger logger = Logger.getLogger(KafkaConsumer.class.getName());

    public static void main(String[] args) {

        System.out.println("Kafka Consumer");

        Properties propertiesIntraday = new Properties();
        propertiesIntraday.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propertiesIntraday.put(ConsumerConfig.GROUP_ID_CONFIG, "group-intraday");
        propertiesIntraday.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesIntraday.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumerGroupIntraday = new KafkaConsumer<>(propertiesIntraday);

        TopicPartition partition0 = new TopicPartition("my_topic2", 0);
        consumerGroupIntraday.assign(Collections.singletonList(partition0));

        Thread groupIntradayThread= new Thread(() -> {

            while (true) {

                System.out.println("Intraday Polling...");

                ConsumerRecords<String, String> records = consumerGroupIntraday.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    JSONObject message = new JSONObject(record.value());
                    addMessageInCsv(message, intradayCsv, CSV.INTRADAY);
                }

            }

        });

        groupIntradayThread.start();

        Properties propertiesDaily = new Properties();
        propertiesDaily.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propertiesDaily.put(ConsumerConfig.GROUP_ID_CONFIG, "group-daily");
        propertiesDaily.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesDaily.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumerGroupDaily = new KafkaConsumer<>(propertiesDaily);

        TopicPartition partition1 = new TopicPartition("my_topic2", 1);
        consumerGroupDaily.assign(Collections.singletonList(partition1));

        Thread groupDailyThread = new Thread(() -> {

            while (true) {

                System.out.println("Daily Polling...");

                ConsumerRecords<String, String> records = consumerGroupDaily.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    JSONObject message = new JSONObject(record.value());
                    addMessageInCsv(message, dailyCsv, CSV.DAILY);
                }

            }
        });

        groupDailyThread.start();

        Properties propertiesDailyAdjusted = new Properties();
        propertiesDailyAdjusted.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propertiesDailyAdjusted.put(ConsumerConfig.GROUP_ID_CONFIG, "group-daily-adjusted");
        propertiesDailyAdjusted.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesDailyAdjusted.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumerGroupDailyAdjusted = new KafkaConsumer<>(propertiesDailyAdjusted);

        TopicPartition partition2 = new TopicPartition("my_topic2", 2);
        consumerGroupDailyAdjusted.assign(Collections.singletonList(partition2));

        Thread dailyAdjustedThread = new Thread(() -> {

            while (true) {

                System.out.println("Polling...");

                ConsumerRecords<String, String> records = consumerGroupDailyAdjusted.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    JSONObject message = new JSONObject(record.value());
                    addMessageInCsv(message, dailyAdjustedCsv, CSV.DAILY_ADJUSTED);
                }
            }
        });

        dailyAdjustedThread.start();
    }

    private static void addMessageInCsv(JSONObject message, String filename, Enum csvEnum){

        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filename, true))){

            StringBuilder lineBuilder = new StringBuilder();

            if((message != null && message.length() != 0) && new File(filename).length() == 0){
                System.out.println("Ovde treba da udjem jednom");
                if(csvEnum == CSV.INTRADAY || csvEnum == CSV.DAILY){
                    lineBuilder.append("timestamp,open,high,low,close,volume");
                } else {
                    lineBuilder.append("timestamp,open,high,low,close,volume,adjustedClose,dividendAmount,splitCoefficient");
                }
                lineBuilder.append(System.lineSeparator());
            } else if ((message != null && message.length() != 0)){

                lineBuilder.append(message.getString("timestamp")).append(",");
                lineBuilder.append(message.getString("open")).append(",");
                lineBuilder.append(message.getString("high")).append(",");
                lineBuilder.append(message.getString("low")).append(",");
                lineBuilder.append(message.getString("close")).append(",");
                lineBuilder.append(message.getString("volume"));

                if(csvEnum == CSV.DAILY_ADJUSTED){
                    lineBuilder.append(",");
                    lineBuilder.append(message.getString("adjustedClose")).append(",");
                    lineBuilder.append(message.getString("dividendAmount")).append(",");
                    lineBuilder.append(message.getString("splitCoefficient"));
                }
                lineBuilder.append(System.lineSeparator());
            }

            bufferedWriter.write(lineBuilder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
