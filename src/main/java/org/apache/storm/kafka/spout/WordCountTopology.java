import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.example.demo.bolt.SplitSentenceBolt;
import org.example.demo.bolt.WordCountBolt;
import org.example.demo.bolt.WriteRedisBolt;


public class WordCountTopology {

    public static void main(String[] args) throws Exception {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .setPassword("123456")
                .setTimeout(3000)
                .build();

        String topic = "test";

        ByTopicRecordTranslator<String,String> brt = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.value(), r.topic()),
                new Fields("sentence", topic));

        brt.forTopic(topic, (r) -> new Values(r.value(), r.topic()), new Fields("sentence", topic));

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
                .builder("localhost:9092", topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
                .setRecordTranslator(brt)
                .build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout);
        builder.setBolt("split-bolt", new SplitSentenceBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-count-bolt", new WordCountBolt()).shuffleGrouping("split-bolt");
        builder.setBolt("write-redis-bolt", new WriteRedisBolt(jedisPoolConfig)).globalGrouping("word-count-bolt");
        StormTopology topology = builder.createTopology();

        Config config = new Config();

            config.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, topology);
        } else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }
    }
}