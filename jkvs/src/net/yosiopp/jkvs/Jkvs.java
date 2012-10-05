package net.yosiopp.jkvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public class Jkvs {
	static final int DEFAULT_PORT = 59128;
	private final String host;
	private final int port;

	public Jkvs(){
		this("localhost", DEFAULT_PORT);
	}

	public Jkvs(String host){
		this(host, DEFAULT_PORT);
	}

	public Jkvs(String host, int port){
		this.host = host;
		this.port = port;
	}

	/**
	 * Jkvsサーバとの通信処理
	 * @param request リクエスト情報
	 * @return レスポンス情報
	 */
	private JkvsResponse communicate(JkvsRequest request){
		JkvsResponse res = null;
		Socket socket = null;
		try {
			socket = new Socket(host, port);

			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(request);
			oos.flush();

			BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
			ObjectInputStream ois = new ObjectInputStream(bis);
			res = (JkvsResponse) ois.readObject();

			ois.close();
			oos.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (socket != null && !socket.isClosed()) {
					socket.close();
				}
			} catch (IOException e) {
			}
		}
		return res;
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String key){
		T value = null;
		JkvsRequest req = new JkvsRequest(JkvsRequest.METHOD.GET);
		req.setKey(key);
		JkvsResponse res = communicate(req);
		if(res != null){
			try {
				value = (T)b2o(res.getValue());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return value;
	}

	public void put(String key, Object value){
		try {
			JkvsRequest req = new JkvsRequest(JkvsRequest.METHOD.PUT);
			req.setKey(key);
			req.setValue(o2b(value));
			communicate(req);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T delete(String key){
		T value = null;
		JkvsRequest req = new JkvsRequest(JkvsRequest.METHOD.DELETE);
		req.setKey(key);
		JkvsResponse res = communicate(req);
		if(res != null){
			try {
				value = (T)b2o(res.getValue());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public List<String> list(){
		List<String> value = null;
		JkvsRequest req = new JkvsRequest(JkvsRequest.METHOD.LIST);
		JkvsResponse res = communicate(req);
		if(res != null){
			try {
				value = (List<String>)b2o(res.getValue());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return value;
	}

	public int count(){
		Integer value = null;
		JkvsRequest req = new JkvsRequest(JkvsRequest.METHOD.COUNT);
		JkvsResponse res = communicate(req);
		if(res != null){
			try {
				value = (Integer)b2o(res.getValue());
				return value.intValue();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}


	final static byte[] o2b(Object object) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bos);
		out.writeObject(object);
		byte[] bytes = bos.toByteArray();
		out.close();
		bos.close();
		return bytes;
	}

	final static Object b2o(byte[] bytes) throws IOException, ClassNotFoundException {
		return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
	}
}
