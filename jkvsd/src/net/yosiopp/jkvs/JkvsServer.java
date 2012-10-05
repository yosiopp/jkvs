package net.yosiopp.jkvs;

import static net.yosiopp.jkvs.JkvsRequest.METHOD.COUNT;
import static net.yosiopp.jkvs.JkvsRequest.METHOD.DELETE;
import static net.yosiopp.jkvs.JkvsRequest.METHOD.GET;
import static net.yosiopp.jkvs.JkvsRequest.METHOD.KILL;
import static net.yosiopp.jkvs.JkvsRequest.METHOD.LIST;
import static net.yosiopp.jkvs.JkvsRequest.METHOD.PUT;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.yosiopp.jkvs.JkvsRequest.METHOD;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JkvsServer {
	static final Logger logger = LoggerFactory.getLogger(JkvsServer.class);
	static final String VERSION = "0.01.002";
	static final String CONF = "jkvsd.conf";

	static ConcurrentHashMap<String, byte[]> map = null;
	static ExecutorService svc = null;
	static boolean halt = false;
	static ServerSocket serverSocket = null;
	static Properties prop = new Properties();

	public static void main(String args[]) {
		// confファイルの読み込み
		try {
			prop.load(new FileInputStream(CONF));
		} catch (Exception e) {
			logger.error("confファイルの読み込みに失敗しました");
			return;
		}

		// 起動パラメタ別操作
		for(String arg : args){
			if("start".equals(arg)){
				start();
				return;
			}
			if("stop".equals(arg)){
				stop();
				return;
			}
		}
		// バージョンを表示して終了
		version();
	}

	/**
	 * バージョン情報を表示する
	 */
	static void version(){
		System.out.println(JkvsServer.class.getSimpleName() + " " + VERSION);
	}

	/**
	 * KVSサーバを開始する
	 */
	static void start(){
		int port = -1;
		try {
			map = new ConcurrentHashMap<String, byte[]>(getCapacity());
			svc = Executors.newCachedThreadPool();
			serverSocket = new ServerSocket(Jkvs.DEFAULT_PORT);
			port = serverSocket.getLocalPort();
			logger.info("jkvs(" + port + ") start -- " + VERSION);
			while(!halt){
				if(!serverSocket.isClosed()){
					Socket socket = serverSocket.accept();
					svc.execute(new Task(socket));
				}
			}
		} catch (Exception e) {
			if(!halt){
				e.printStackTrace();
			}
		} finally {
			try {
				if(svc != null){
					svc.shutdownNow();
				}
				if (serverSocket != null) {
					serverSocket.close();
				}
			} catch (IOException e) {
			}
		}
		logger.info("jkvs(" + port + ") shutdown");
	}

	/**
	 * KVSサーバを終了する
	 */
	static void stop(){
		Socket socket = null;
		try {
			socket = new Socket("127.0.0.1", Jkvs.DEFAULT_PORT);
			JkvsRequest request = new JkvsRequest(METHOD.KILL);
			// 削除パスを指定する
			request.setValue(prop.getProperty("pass").getBytes());

			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(request);
			oos.flush();
			oos.close();
			logger.info("jkvs stop");

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
	}

	static int getCapacity() {
		try{
			return Integer.parseInt((String) prop.get("capacity"));
		}catch (Exception e) {
		}
		return 100;
	}

	static void debug(Socket so, String st){
		if(logger.isDebugEnabled()){
			logger.debug("[" + ((so != null)?so.hashCode():"") + "] " + st);
		}
	}

	/**
	 * 通信処理タスク
	 */
	static class Task implements Runnable{
		Socket socket;

		public Task(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run(){
			try{
				Thread.sleep(0);

				int status = 200;
				byte[] value = null;

				BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
				ObjectInputStream ois = new ObjectInputStream(bis);
				JkvsRequest request = (JkvsRequest) ois.readObject();

				if(GET == request.getMethod()){
					debug(socket, "get " + request.getKey());
					// 値を返却
					value = map.get(request.getKey());
				}
				else if(PUT == request.getMethod()){
					debug(socket, "put " + request.getKey() + "(" + request.getValue().length + ")");
					// 値を設定
					map.put(request.getKey(), request.getValue());
				}
				else if(DELETE == request.getMethod()){
					debug(socket, "delete " + request.getKey());
					// 値を削除
					// 削除した値を返却
					value = map.remove(request.getKey());
				}
				else if(LIST == request.getMethod()){
					debug(socket, "list");
					// リストを返却
					List<String> list = new ArrayList<String>();
					Enumeration<String> em = map.keys();
					while(em.hasMoreElements()){
						list.add(em.nextElement());
					}
					value = Jkvs.o2b(list);
				}
				else if(COUNT == request.getMethod()){
					// 個数を返却
					logger.debug("count:"+map.size());
					value = Jkvs.o2b(new Integer(map.size()));
				}
				else if(KILL == request.getMethod()){
					String pass = new String(request.getValue());
					// ローカルからの接続でかつpassが一致した場合のみサーバ終了コマンドを許可する
					if("127.0.0.1".equals(socket.getInetAddress().getHostName())
						&& prop.getProperty("pass").equals(pass)){
						// サーバーを終了
						halt = true;
						serverSocket.close();
						serverSocket = null;
						svc.shutdownNow();
					}
				}
				else{
					// bad request
					status = 400;
				}

				Thread.sleep(0);

				JkvsResponse response = new JkvsResponse();
				response.setStatus(status);
				response.setValue(value);

				// 出力
				BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(response);
				oos.flush();

				oos.close();
				ois.close();

			}catch (InterruptedException ie) {
				debug(socket, "interrupted");
			}catch (Exception e) {
				if(!halt){
					e.printStackTrace();
				}
			}finally{
				if(socket != null && !socket.isClosed()){
					try {
						socket.close();
						debug(socket, "close");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}