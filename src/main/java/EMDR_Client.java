/**
 * Created by kknauf on 31.01.16.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.zeromq.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.zip.*;

public class EMDR_Client {

    public static final String EVE_RELAY = "tcp://relay-us-central-1.eve-emdr.com:8050";
    public static final String KAFKA_BROKERS= "localhost:9092";

    private static KafkaProducer kafkaProducer;

    public static void main(String[] args) throws Exception {

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);

        // Connect to the first publicly available relay.
        subscriber.connect(EVE_RELAY);
        subscriber.subscribe(new byte[0]);

        createKafkaProducerFromProgramArgs(KAFKA_BROKERS);

        while (true) {
            try {
                // Receive compressed raw market data.
                JSONObject market_data = getNextJsonFromSocket(subscriber);

                List<String> orderEvents = getOrderJsonStrings(market_data);

                writeStringsToKafkaTopic(orderEvents, "orders");

            } catch (ZMQException ex) {
                System.out.println("ZMQ Exception occurred : " + ex.getMessage());
            }
        }
    }

    private static void createKafkaProducerFromProgramArgs(String servers) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", servers);
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer(kafkaProps);
    }

    private static JSONObject getNextJsonFromSocket(ZMQ.Socket subscriber) throws DataFormatException, UnsupportedEncodingException, ParseException {
        byte[] receivedData = subscriber.recv(0);

        // We build a large enough buffer to contain the decompressed data.
        byte[] decompressed = new byte[receivedData.length * 16];

        // Decompress the raw market data.
        Inflater inflater = new Inflater();
        inflater.setInput(receivedData);
        int decompressedLength = inflater.inflate(decompressed);
        inflater.end();

        byte[] output = new byte[decompressedLength];
        System.arraycopy(decompressed, 0, output, 0, decompressedLength);

        String market_json = new String(output, "UTF-8");

        // Un-serialize the JSON data.
        JSONParser parser = new JSONParser();
        return (JSONObject)parser.parse(market_json);
    }

    private static void writeStringsToKafkaTopic(List<String> strings, String topic) {
        for(String string: strings) {
            kafkaProducer.send(new ProducerRecord(topic, null,string));
        }

    }

    private static List<String> getOrderJsonStrings(JSONObject market_data) {
        JSONArray rowSets = (JSONArray)market_data.get("rowsets");
        JSONArray columns = (JSONArray)market_data.get("columns");
        List<String> orderEvents = new ArrayList<String>();
        for (Object rowSet: rowSets) {
            JSONObject rowsetJson = (JSONObject) rowSet;

            Long regionId = (Long) rowsetJson.get("regionID");
            Long typeId = (Long) rowsetJson.get("typeID");

            JSONArray rows = (JSONArray) rowsetJson.get("rows");

            for (Object row : rows) {
                JSONArray rowJson = (JSONArray) row;
                Map<String, Object> outputRow = new HashMap<String, Object>();
                for (int i=0; i<rowJson.size(); i++) {
                    outputRow.put(columns.get(i).toString(), rowJson.get(i));
                }
                outputRow.put("regionID", regionId.toString());
                outputRow.put("typeID", typeId.toString());
                orderEvents.add(JSONObject.toJSONString(outputRow));
            }
        }
        return orderEvents;
    }
}

