/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package storm.kafka;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.spout.Scheme;
	  
public class KafkaTopology {
	
	public static class MessageScheme implements Scheme { 
	    
	    /* (non-Javadoc)
	     * @see backtype.storm.spout.Scheme#deserialize(byte[])
	     */
	    public List<Object> deserialize(byte[] ser) {
	        try {
	            String msg = new String(ser, "UTF-8"); 
	            return new Values(msg);
	        } catch (UnsupportedEncodingException e) {  
	         
	        }
	        return null;
	    }
	    
	    
	    /* (non-Javadoc)
	     * @see backtype.storm.spout.Scheme#getOutputFields()
	     */
	    public Fields getOutputFields() {
	        // TODO Auto-generated method stub
	        return new Fields("msg");  
	    }  
	} 

	public static class SequenceBolt extends BaseBasicBolt{
	    
	    /* (non-Javadoc)
	     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
	     */
	    public void execute(Tuple input, BasicOutputCollector collector) {
	        // TODO Auto-generated method stub
	         String word = (String) input.getValue(0);  
	         String out = "I'm " + word +  "!";  
	         System.out.println("out=" + out);
	         collector.emit(new Values(out));
	    }
	    
	    /* (non-Javadoc)
	     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	     */
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("message"));
	    }
	}
	
	public static class KafkaWordSplitter extends BaseRichBolt{
		private OutputCollector collector;
		  
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;		    
		}		
		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			String[] words = line.split("\\s+");
			for(String word : words) {
				collector.emit(input, new Values(word, 1));
			}
			collector.ack(input);
		}	
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));	    
		}		      
	}
	  
	public static class SplitSentence extends ShellBolt implements IRichBolt {

	    public SplitSentence() {
	      super("python", "splitsentence.py");
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word"));
	    }

	    @Override
	    public Map<String, Object> getComponentConfiguration() {
	      return null;
	    }
	}

	public static class WordCount extends BaseBasicBolt {
	    Map<String, Integer> counts = new HashMap<String, Integer>();

	    @Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      String word = tuple.getString(0);
	      Integer count = counts.get(word);
	      if (count == null)
	        count = 0;
	      count++;
	      counts.put(word, count);
	      collector.emit(new Values(word, count));
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word", "count"));
	    }
	}
	  
	
	public static void main(String[] args) throws Exception {
    	String zks = "192.168.1.57:2181";
    	String topicName = "stormone";
    	BrokerHosts brokerHosts = new ZkHosts(zks);
    	SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.socketTimeoutMs = 1000000;
        kafkaConfig.fetchMaxWait = 100000;
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout, 3);
        //builder.setBolt("bolt", new SequenceBolt()).shuffleGrouping("spout");
        /*builder.setBolt("printer", new PrinterBolt(), 4)
                .shuffleGrouping("spout");
        builder.setBolt("printer2", new PrinterBolt(), 4)
        .shuffleGrouping("spout");*/
        builder.setBolt("word-splitter", new KafkaWordSplitter(), 3).shuffleGrouping("spout");
        //builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        //builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
        
        Config config = new Config();
        config.setDebug(false);
        
        if(args!=null && args.length > 0) {
            config.setNumWorkers(2);
            
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {        
            config.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, builder.createTopology());
        
            Thread.sleep(10000);

            cluster.shutdown();
        }
       
    }
    
}
