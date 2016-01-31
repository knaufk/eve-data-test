/**
 * Created by kknauf on 31.01.16.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeromq.*; // https://github.com/zeromq/jzmq
import org.json.simple.*; // http://code.google.com/p/json-simple/downloads/list
import org.json.simple.parser.*;
import java.util.zip.*;

public class EMDR_Client {

    public static void main(String[] args) throws Exception {

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket subscriber = context.socket(ZMQ.SUB);

        // Connect to the first publicly available relay.
        subscriber.connect("tcp://relay-us-central-1.eve-emdr.com:8050");

        // Disable filtering.
        subscriber.subscribe(new byte[0]);

        while (true) {
            try {
                // Receive compressed raw market data.
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

                // Transform data into JSON strings.
                String market_json = new String(output, "UTF-8");

                ObjectMapper mapper = new ObjectMapper();

                // Un-serialize the JSON data.
                JSONParser parser = new JSONParser();
                JSONObject market_data = (JSONObject)parser.parse(market_json);
                Object json = mapper.readValue(market_json, Object.class);
                // Dump the market data to console or, you know, do more fun things here.
                System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

            } catch (ZMQException ex) {
                System.out.println("ZMQ Exception occurred : " + ex.getMessage());
            }
        }
    }
}

