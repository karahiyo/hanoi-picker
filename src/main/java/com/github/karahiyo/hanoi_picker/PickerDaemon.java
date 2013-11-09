package com.github.karahiyo.hanoi_picker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
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

	/** server socket */
	private DatagramSocket serverSocket;
	
	/** messsage send host */
	private SocketAddress socketAddress;
	
	/** log output directory. default "/tmp" */
	private String outdir = "/tmp";
	private final String outfile = "hanoi-trace.log";

	/** set port */
	private final int PORT = 9999;
	private static final int MINIMUM_PORT_NUMBER = 50000;
	private static final int MAXMUM_PORT_NUMBER = 65535;

	/** a flag for receiving data from client now. */
	private boolean         isRecievedNow;

	/** a flag for receiving terminate message. */
	private boolean         isTermination;

	/** server socket timeout(ms) */
	public final int    TIMEOUT_SERVER_SOCKET     = 5000;

	/** daemon start time */
	private long 	DAEMON_START_TIME; 
	private long 	LAST_HIST_OUT_TIME;

	/** hist out interval */
	public static final int HIST_OUT_INTETRVAL = 5000; //ms

	/** main histogram data */
	public Map<String, Long> hist = new HashMap<String, Long>();

	// construct
	public PickerDaemon() {}
	
	/**
	 * setup socket connection
	 * @param 
	 * @throws IOException
	 */
	public void setupSocket() throws IOException {
		this.serverSocket = new DatagramSocket(this.PORT);
		this.serverSocket.setSoTimeout(this.TIMEOUT_SERVER_SOCKET);
	}
	
	/**
	 * setup socket connection 
	 * @param port
	 * @throws IOException
	 */
	public void setupSocket(int port) throws IOException {
		this.serverSocket = new DatagramSocket(port);
		this.serverSocket.setSoTimeout(this.TIMEOUT_SERVER_SOCKET);
	}
	
	public void setupOutDir(String file) throws IOException {
		this.outdir = file;
	}

	public void run() {

		DAEMON_START_TIME = System.currentTimeMillis();
		LAST_HIST_OUT_TIME = System.currentTimeMillis();

		while ( ! this.isTermination) {

			byte[] buf = new byte[256];
			DatagramPacket packet = new DatagramPacket(buf, buf.length);
			
			try {
				/** 受信 & wait */
				serverSocket.receive(packet);
				/** 送信元情報取得 */
				socketAddress = packet.getSocketAddress();
			} catch (SocketTimeoutException timeExc) {
				// nothing to do
			} catch (IOException e) {
				System.out.println(e);
			}

			// periodical output hist 
			if(LAST_HIST_OUT_TIME + HIST_OUT_INTETRVAL <= System.currentTimeMillis() ) {
				long timestamp = LAST_HIST_OUT_TIME+ HIST_OUT_INTETRVAL; 
				String json = makeJsonString(timestamp, hist);

				// debug
				System.out.println(json);

				PrintWriter pw;
				try {
					File log_file = new File(this.outdir + "/" + this.outfile);
					pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(log_file, true)));
					pw.println(json);
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				/** update */
				LAST_HIST_OUT_TIME += HIST_OUT_INTETRVAL;
				hist.clear();
			}

			/** 受信バイト数取得 */
			int len = packet.getLength();

			// not accepting data
			if ( len == 0) {
				continue;
			}

			try {
				String msg = new String(buf,0,len);
				while ( ! msg.equals("") ) {
					if ( hist.containsKey(msg) ) {
						hist.put(msg, hist.get(msg) + 1);
					} else {
						hist.put(msg, 1L);
					}
				}

			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				/* close child process */
				serverSocket.close();
			}
		}
	}

	public String makeJsonString(long time, Map<String, Long> hist) {
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("time", time);
		map.put("keymap", hist);
		long sum = countAllFreq(hist);
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
	
	public long countAllFreq(Map<String, Long> map) {
		int sum = 0;
		for (Map.Entry<String, Long> e : map.entrySet()) {
			sum += e.getValue();
		}
		return sum;
	}
}

