package net.yosiopp.jkvs;

import java.io.Serializable;

class JkvsResponse implements Serializable{
	private static final long serialVersionUID = 1L;
	private int status;
	private byte[] value;
	public void setStatus(int status) {
		this.status = status;
	}
	public int getStatus() {
		return status;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	public byte[] getValue() {
		return value;
	}
}
