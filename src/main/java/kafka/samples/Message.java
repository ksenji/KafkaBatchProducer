package kafka.samples;

@org.msgpack.annotation.Message
public class Message {

	private String line;

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}
}
