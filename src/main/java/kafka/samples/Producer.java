package kafka.samples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.msgpack.MessagePack;

import kafka.samples.KafkaBatchProducer.Callback;
import kafka.samples.KafkaBatchProducer.RecordMetadataAndException;

public class Producer {

	private String brokers;

	public Producer(String brokers) {
		this.brokers = brokers;
	}

	public void produce(int n) throws Exception {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		KafkaBatchProducer<String, byte[]> producer = new KafkaBatchProducer<String, byte[]>(properties);
		try {
			MessagePack msgpack = new MessagePack();
			int b = n;
			final CountDownLatch latch = new CountDownLatch(b);
			while (n > 0) {
				int batchSize = 50;
				List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
				for (int i = 0; i < batchSize; i++) {
					Message message = new Message();
					message.setLine("Message: " + ((b - n) * batchSize + i));
					ProducerRecord<String, byte[]> record = new ProducerRecord<>("security.source",
							msgpack.write(message));
					records.add(record);
				}
				producer.send(records, new Callback() {
					@Override
					public void onCompletion(List<RecordMetadataAndException> tuples) {
						System.out.println(tuples.size());
						latch.countDown();
					}
				});
				n--;
			}
			latch.await();
		} finally {
			producer.close();
		}
	}

	public static void main(String[] args) throws Exception {
		new Producer("10.65.206.128:9092,10.65.206.122:9092,10.65.206.12:9092").produce(10);
	}
}
