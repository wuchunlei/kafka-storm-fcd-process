package storm.kafka.tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSON;

public class StormProducer {
	class GPSInfo {
		private String index;
		private String Lon;
		private String Lat;
		
		public String getIndex() {return index;}
		public void setINdex(String index) {this.index = index;}
		
		public String getLon() {return Lon;}
		public void setLon(String Lon) {this.Lon = Lon;}
		
		public String getLat() {return Lat;}
		public void setLat(String Lat) {this.Lat = Lat;}
	}
	
	class Car {
		private List<GPSInfo> location = new ArrayList<GPSInfo>();
		
		public List<GPSInfo> getInfo() {return location;}
		public void setInfo(List<GPSInfo> location) {this.location = location;}
	}

    private static String[] sentences = new String[]{
            "the cow jumped over the moon",
            "the man went to the store and bought some candy",
            "four score and seven years ago",
            "how many apples can you eat",
    };
    
     public String BuildJson() throws JSONException{
    	Car car = new Car();
    	GPSInfo gps = new GPSInfo();
    	gps.setINdex("1");
    	gps.setLon("139.40");
    	gps.setLat("40.21");
    	GPSInfo gps2 = new GPSInfo();
    	gps2.setINdex("2");
    	gps2.setLon("139.41");
    	gps2.setLat("40.22");
    	car.getInfo().add(gps);
    	car.getInfo().add(gps2);
    	String jsonString = JSON.toJSONString(car);
    	return jsonString;
    }
    
    public static void main(String args[]) throws InterruptedException {
        Properties props = new Properties();

        props.put("metadata.broker.list", args[0]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        //props.put("request.timeout.ms", "60000");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        StormProducer stormproducer = new StormProducer();
        String stroutput = stormproducer.BuildJson();
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
