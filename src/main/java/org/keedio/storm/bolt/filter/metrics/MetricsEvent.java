package org.keedio.storm.bolt.filter.metrics;

/**
 * 
 * Definition of event types used for management by the controller metric metric.
 *
 */
public class MetricsEvent {
	
	public static final int UPDATE_THROUGHPUT = 11;
	
	private int code;
	private long value = -1;
	private String str;
	
	public String getStr() {
		return str;
	}

	public void setStr(String str) {
		this.str = str;
	}

	public MetricsEvent(int code) {
		this.code = code;
	}
	
	public MetricsEvent(int code, long value) {
		this.code = code;
		this.value = value;
	}

	public MetricsEvent(int code, String str) {
		this.code = code;
		this.str = str;
	}

	public int getCode() {
		return code;
	}
	
	public long getValue() {
		return this.value;
	}

}
