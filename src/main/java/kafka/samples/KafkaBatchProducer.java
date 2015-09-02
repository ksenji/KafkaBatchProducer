package kafka.samples;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaBatchProducer<K, V> extends KafkaProducer<K, V> {

	public KafkaBatchProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		super(configs, keySerializer, valueSerializer);
	}

	public KafkaBatchProducer(Map<String, Object> configs) {
		super(configs);
	}

	public KafkaBatchProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		super(properties, keySerializer, valueSerializer);
	}

	public KafkaBatchProducer(Properties properties) {
		super(properties);
	}

	public void send(List<ProducerRecord<K, V>> records, Callback cb) {
		BatchCallbackHelper helper = new BatchCallbackHelper(records.size(), cb);
		for (ProducerRecord<K, V> record : records) {
			org.apache.kafka.clients.producer.Callback _cb = null;
			try {
				_cb = helper.newInstance();
				send(record, _cb);
			} catch (Exception e) {
				_cb.onCompletion(null, e);
			}
		}
	}

	public interface Callback {
		public void onCompletion(List<RecordMetadataAndException> tuples);
	}

	public static final class RecordMetadataAndException {
		private final RecordMetadata metadata;
		private final Exception exception;

		public RecordMetadataAndException(RecordMetadata metadata, Exception exception) {
			this.metadata = metadata;
			this.exception = exception;
		}

		public RecordMetadata getMetadata() {
			return metadata;
		}

		public Exception getException() {
			return exception;
		}
	}
}
