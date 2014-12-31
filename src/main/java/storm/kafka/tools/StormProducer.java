package storm.kafka.tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;


public class StormProducer {


    private static String[] sentences = new String[]{
            "the cow jumped over the moon",
            "the man went to the store and bought some candy",
            "four score and seven years ago",
            "how many apples can you eat",
    };
    
    public static String BuildJson() throws JSONException {
    	JSONArray list = new JSONArray();
    	String str = "{'index':'1', 'Lon':'139.40', 'Lat':'40.21'}";
    	list.add(str);
    	str = "{'index':'2', 'Lon':'139.41', 'Lat':'40.22'}";
    	list.add(str);
    	return list.toJSONString();
    }
    
    public static void main(String args[]) throws InterruptedException {
        Properties props = new Properties();

        props.put("metadata.broker.list", args[0]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        //props.put("request.timeout.ms", "60000");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        String stroutput = BuildJson();
        while (true) {
        	KeyedMessage<String, String> data = new KeyedMessage<String, String>("stormone", stroutput);
            producer.send(data);
            Thread.sleep(100);
            /*for (String sentence : sentences) {
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("stormone", sentence);
                producer.send(data);
                Thread.sleep(100);
            }*/
        }

    }
}
