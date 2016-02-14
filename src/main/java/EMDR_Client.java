/**
 * Created by kknauf on 31.01.16.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeromq.*; // https://github.com/zeromq/jzmq
import org.json.simple.*; // http://code.google.com/p/json-simple/downloads/list
import org.json.simple.parser.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

                List<String> orderEvents = getOrderJsonStrings(market_data);

                System.out.println(orderEvents);


            } catch (ZMQException ex) {
                System.out.println("ZMQ Exception occurred : " + ex.getMessage());
            }
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

