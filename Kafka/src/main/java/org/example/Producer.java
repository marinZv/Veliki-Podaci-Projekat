package org.example;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
//import org.apache.kafka.clients.producer.ProducerConfig;

//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;

import java.util.Properties;
import java.util.logging.Logger;


public class Producer {

    private static final String urlIntraday = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=demo";
    private static final String urlDaily = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=demo";
    private static final String urlDailyAdjusted = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&outputsize=full&apikey=demo";

    private static final Logger logger = Logger.getLogger(KafkaProducer.class.getName());



    public static void main(String[] args) {

        System.out.println("Kafka producer!");

        JSONObject dataIntraday = fetchDataFromApi(Producer.urlIntraday, "Time Series (5min)","intraday" );
        JSONObject dataDaily = fetchDataFromApi(Producer.urlDaily, "Time Series (Daily)","daily");
        JSONObject dataDailyAdjusted = fetchDataFromApi(Producer.urlDailyAdjusted, "Time Series (Daily)", "adjustedDaily");



        sendToKafkaTopic(dataIntraday, 0);
        sendToKafkaTopic(dataDaily, 1);
        sendToKafkaTopic(dataDailyAdjusted, 2);

    }

    private static JSONObject fetchDataFromApi(String apiUrl, String jsonKey, String apiType){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(apiUrl);

        try(CloseableHttpResponse response = httpClient.execute(httpGet)){
            String responseBody = EntityUtils.toString(response.getEntity());
//            System.out.println(responseBody);
            return parseResponse(responseBody, jsonKey, apiType);
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JSONObject parseResponse(String responseBody, String jsonKey, String apiType){
        JSONObject jsonResponse = new JSONObject(responseBody);
        JSONObject timeSeries = jsonResponse.getJSONObject(jsonKey);

        JSONObject messages = new JSONObject();

        for(String key: timeSeries.keySet()){
            JSONObject specificData = timeSeries.getJSONObject(key);
            JSONObject message = new JSONObject();

            message.put("timestamp", key);
            message.put("open", specificData.getString("1. open"));
            message.put("high", specificData.getString("2. high"));
            message.put("low", specificData.getString("3. low"));
            message.put("close", specificData.getString("4. close"));

            if(apiType.equalsIgnoreCase("intraday") || apiType.equalsIgnoreCase("daily")){
                message.put("volume", specificData.getString("5. volume"));
            }else if(apiType.equalsIgnoreCase("adjustedDaily")){
                message.put("adjustedClose", specificData.getString("5. adjusted close"));
                message.put("volume", specificData.getString("6. volume"));
                message.put("dividendAmount", specificData.getString("7. dividend amount"));
                message.put("splitCoefficient", specificData.getString("8. split coefficient"));
            }

            messages.put(key, message);
        }

        return messages;
    }

    private static void sendToKafkaTopic(JSONObject data, int partition){

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        org.apache.kafka.clients.producer.Producer producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);

        for(String key : data.keySet()){
            JSONObject message = data.getJSONObject(key);
            ProducerRecord producerRecord = new ProducerRecord("my_topic2", partition, "data_key", message.toString());
            producer.send(producerRecord);
            System.out.println(message);
        }

    }

}