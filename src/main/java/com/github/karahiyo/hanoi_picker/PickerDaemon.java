package com.github.karahiyo.hanoi_picker;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * receive message from PickerClient, and count-up each keys.
 * 
 * @author yu_ke
 *
 */
public class PickerDaemon implements Runnable {

	private DataInputStream is = null;
	private PrintStream os = null;

	/** server socket */
	private ServerSocket serverSocket;

	/** set port */
	private static final int PORT = 9999;
	private static final int MINIMUM_PORT_NUMBER = 1024;
	private static final int MAXMUM_PORT_NUMBER = 65535;

	/** a flag for receiving data from client now. */
	private boolean         isRecievedNow;

	/** a flag for receiving terminate message. */
	private boolean         isTermination;

	/** server socket timeout(ms) */
	public static final int    TIMEOUT_SERVER_SOCKET     = 500;

	/** daemon start time */
	private long 	DAEMON_START_TIME; 
	private long 	LAST_HIST_OUT_TIME;

	/** hist out interval */
	public static final int HIST_OUT_INTETRVAL = 5000; //ms

	/** main histogram data */
	public Map<String, Integer> hist = new HashMap<String, Integer>();

	public PickerDaemon() {
		try {
			this.serverSocket = new ServerSocket(this.PORT);
			this.serverSocket.setSoTimeout(this.TIMEOUT_SERVER_SOCKET);
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	@Override
	public void run() {

		DAEMON_START_TIME = System.currentTimeMillis();
		LAST_HIST_OUT_TIME = System.currentTimeMillis();

		while ( ! this.isTermination) {
			Socket connectedSocket = null;

			try {
				connectedSocket = this.serverSocket.accept();
			} catch (SocketTimeoutException timeExc) {
				// nothing to do
			} catch (IOException e) {
				System.out.println(e);
			}

			// output hist
			if(LAST_HIST_OUT_TIME + HIST_OUT_INTETRVAL <= System.currentTimeMillis() ) {
				long timestamp = LAST_HIST_OUT_TIME+ HIST_OUT_INTETRVAL; 
				String json = makeJsonString(timestamp, hist);
				System.out.println(json);

				/** update */
				LAST_HIST_OUT_TIME += HIST_OUT_INTETRVAL;
				hist.clear();
			}


			// not accepting data
			if ( connectedSocket == null) {
				continue;
			}

			try {
				/* get data input stream of socket */
				is = new DataInputStream(connectedSocket.getInputStream());

				/* get output stream of sub process */
				os  = new PrintStream(connectedSocket.getOutputStream());
				BufferedReader bf = new BufferedReader(new InputStreamReader(is));



				String line;
				while ( (line = bf.readLine()) != null) {
					line = URLDecoder.decode(line, "UTF-8");
					if ( hist.containsKey(line) ) {
						hist.put(line, hist.get(line) + 1);
					} else {
						hist.put(line, 1);
					}
					os.println(line);
				}


				/* close child process */
				if (is != null) is.close();
				if (os != null) os.close();

			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {
					/* close child process */
					if (is != null) is.close();
					if (os != null) os.close();
				} catch (IOException e) {
					System.out.println(e);
				}
			}
		}
	}

	public String makeJsonString(long time, Map<String, Integer> hist) {
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("time", time);
		map.put("keymap", hist);
		int sum = countAllFreq(hist);
		map.put("sum", sum);
		ObjectMapper mapper = new ObjectMapper();
		String json = null;

		try {
			json = mapper.writeValueAsString(map);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	public int countAllFreq(Map<String, Integer> map) {
		int sum = 0;
		for (Map.Entry<String, Integer> e : map.entrySet()) {
			sum += e.getValue();
		}
		return sum;
	}
}

