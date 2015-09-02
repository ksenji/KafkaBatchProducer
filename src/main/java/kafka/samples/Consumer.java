package kafka.samples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.msgpack.MessagePack;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer {

	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("consumerTimeoutMs", "-1");

		return new ConsumerConfig(props);
	}

	public static void main(String[] args) throws Exception {
		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig("10.65.206.117:2181/kafka", "consumer.test"));

		MessagePack msgpack = new MessagePack();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("security.source", new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("security.source");

		for (final KafkaStream<byte[], byte[]> stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				Message m = msgpack.read(it.next().message(), Message.class);
				System.out.println(m.getLine());
			}
		}

		consumer.shutdown();
	}
}
