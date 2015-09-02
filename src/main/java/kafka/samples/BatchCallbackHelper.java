package kafka.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.samples.KafkaBatchProducer.RecordMetadataAndException;

//NotThreadSafe
public class BatchCallbackHelper {

	private int counter = 0;
	private final AtomicInteger pendingCallbacks;
	private final kafka.samples.KafkaBatchProducer.Callback cb;
	private int batchSize;

	private ConcurrentMap<Integer, RecordMetadataAndException> completionMap = new ConcurrentHashMap<>();

	public BatchCallbackHelper(int batchSize, kafka.samples.KafkaBatchProducer.Callback cb) {
		this.pendingCallbacks = new AtomicInteger(batchSize);
		this.cb = cb;
		this.batchSize = batchSize;
	}

	public Callback newInstance() {
		if (counter == batchSize) {
			throw new RuntimeException("Batch size exceeded");
		}
		return new BatchCallback(counter++);
	}

	private class BatchCallback implements Callback {

		private int id;

		public BatchCallback(int id) {
			this.id = id;
		}

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			completionMap.putIfAbsent(Integer.valueOf(id), new RecordMetadataAndException(metadata, exception));
			if (pendingCallbacks.decrementAndGet() == 0) {
				List<RecordMetadataAndException> tuples = new ArrayList<>();
				for (int i = 0; i < batchSize; i++) {
					RecordMetadataAndException tuple = completionMap.get(Integer.valueOf(i));
					tuples.add(tuple);
				}
				cb.onCompletion(tuples);
			}
		}
	}
}
