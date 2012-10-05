package net.yosiopp.jkvs;

import static net.yosiopp.jkvs.JkvsRequest.METHOD.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JkvsServer {
	static final int INITIAL_CAPACITY = 100;
	static ConcurrentHashMap<String, byte[]> map = null;
	static ExecutorService svc = null;
	static boolean alive = true;

	public static void main(String args[]) {
		ServerSocket serverSocket = null;
		int port = -1;
		try {
			map = new ConcurrentHashMap<String, byte[]>(INITIAL_CAPACITY);
			svc = Executors.newCachedThreadPool();
			serverSocket = new ServerSocket(Jkvs.DEFAULT_PORT);
			port = serverSocket.getLocalPort();
			System.out.println("jkvs(" + port + ") start");
			while(alive){
				Socket socket = serverSocket.accept();
				svc.execute(new Task(socket));
			}
			svc.shutdownNow();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (serverSocket != null) {
					serverSocket.close();
				}
			} catch (IOException e) {
			}
		}
		System.out.println("jkvs(" + port + ") shutdown");
	}

	static void log(Socket so, String st){
		System.out.println("[" + ((so != null)?so.hashCode():"") + "] " + st);
	}

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
					log(socket, "get " + request.getKey());
					// 値を返却
					value = map.get(request.getKey());
				}
				else if(PUT == request.getMethod()){
					log(socket, "put " + request.getKey() + "(" + request.getValue().length + ")");
					// 値を設定
					map.put(request.getKey(), request.getValue());
				}
				else if(DELETE == request.getMethod()){
					log(socket, "delete " + request.getKey());
					// 値を削除
					// 削除した値を返却
					value = map.remove(request.getKey());
				}
				else if(LIST == request.getMethod()){
					log(socket, "list");
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
					value = Jkvs.o2b(new Integer(map.size()));
				}
				else if(KILL == request.getMethod()){
					// サーバーを終了
					alive = false;
					return;
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
				ie.printStackTrace();
			}catch (IOException ioe) {
				ioe.printStackTrace();
			}catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(socket != null && !socket.isClosed()){
					try {
						socket.close();
						log(socket, "close");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}