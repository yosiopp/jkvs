package net.yosiopp.jkvs;

import java.io.Serializable;

class JkvsRequest implements Serializable{
	private static final long serialVersionUID = 1L;

	public static enum METHOD{
		GET, PUT, DELETE, LIST, COUNT,
		KILL
	}

	public JkvsRequest(METHOD method){
		this.method = method;
	}

	private METHOD method;
	private String key;
	private byte[] value;

	public void setMethod(METHOD method) {
		this.method = method;
	}
	public METHOD getMethod() {
		return method;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getKey() {
		return key;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	public byte[] getValue() {
		return value;
	}
}
