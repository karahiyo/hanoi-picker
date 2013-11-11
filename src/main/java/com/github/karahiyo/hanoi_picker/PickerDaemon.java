package com.github.karahiyo.hanoi_picker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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
	private final String map_outfile = "hanoi-map-trace.log";
	private final String shuffle_outfile = "hanoi-shffle-trace.log";

	/** パケットサイズ */
	public final int PACKET_SIZE = 1024;

	/** set port */
	private final int PORT = 55000;
	private static final int MINIMUM_PORT_NUMBER = 50000;
	private static final int MAXMUM_PORT_NUMBER = 65535;

	/** a flag for receiving terminate message. */
	private boolean         isTermination = false;

	/** server socket timeout(ms) */
	public final int    TIMEOUT_SERVER_SOCKET     = 5000;

	/** daemon start time */
	private long 	DAEMON_START_TIME; 
	private long 	LAST_HIST_OUT_TIME;

	/** hist out interval */
	public static final int HIST_OUT_INTETRVAL = 1000; //ms

	/** map histogram data */
	public Map<String, Long> map_hist = new HashMap<String, Long>();

	/** reduce histogram data */
	public Map<String, Long> shuffle_hist = new HashMap<String, Long>();

    public static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

   
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
		System.out.println("DatagramSocket running: port=" + serverSocket.getLocalPort());
	}

	/**
	 * setup socket connection 
	 * @param port
	 * @throws IOException
	 */
	public void setupSocket(int port) throws IOException {
		this.serverSocket = new DatagramSocket(port);
		this.serverSocket.setSoTimeout(this.TIMEOUT_SERVER_SOCKET);
		System.out.println("DatagramSocket running: port=" + serverSocket.getLocalPort());
	}

	public void setupOutDir(String dir) throws IOException {
		this.outdir = dir;
	}

	public void run() {
		System.out.println("** START PickerDaemon");

		DAEMON_START_TIME = System.currentTimeMillis();
		LAST_HIST_OUT_TIME = System.currentTimeMillis();

		while ( ! this.isTermination) {


			/** 
			 * 受信データ格納変数を初期化
			 */
			byte[] buf = new byte[this.PACKET_SIZE];
			DatagramPacket packet = new DatagramPacket(buf, buf.length);
			int len = 0;			
			String msg = "";


			// periodical output hist 
			if(LAST_HIST_OUT_TIME + HIST_OUT_INTETRVAL <= System.currentTimeMillis() ) {
				long timestamp = LAST_HIST_OUT_TIME+ HIST_OUT_INTETRVAL; 
				String map_json = makeJsonString(timestamp, map_hist);
				String shuffle_json = makeJsonString(timestamp, shuffle_hist);

				// debug
				System.out.println(map_json);
				System.out.println(shuffle_json);

				PrintWriter map_pw;
				PrintWriter shuffle_pw;
				try {
					File map_log_file = new File(this.outdir + "/" + this.map_outfile);
					File shuffle_log_file = new File(this.outdir + "/" + this.shuffle_outfile);
					map_pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(map_log_file, true)));
					shuffle_pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(shuffle_log_file, true)));
					map_pw.println(map_json);
					shuffle_pw.println(shuffle_json);
					map_pw.close();
					shuffle_pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				/** update */
				LAST_HIST_OUT_TIME += HIST_OUT_INTETRVAL;
				map_hist.clear();
				shuffle_hist.clear();
				map_json = "";
				shuffle_json = "";
			}

			/**
			 * 受信処理
			 */
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

			/** 受信バイト数取得 */
			len = packet.getLength();
			try {
				msg = new String(buf, 0, len, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

			try {
				// not accepting data
				if ( msg == null || msg.equals("") || msg.length() == 1024  || this.socketAddress == null) {
					continue;
				} else {
					msg = msg.toString();
					
					// divide string 
					String[] data = msg.split(",", 2);
					String phase = data[0];
					msg = data[1];

					if (phase.equals("MAP")) {
						
						if ( map_hist.containsKey(msg) ) {
							map_hist.put(msg, map_hist.get(msg) + 1);
						} else {
							map_hist.put(msg, 1L);
						}
					} else if (phase.equals("SHUFFLE")) {
						if ( shuffle_hist.containsKey(msg) ) {
							shuffle_hist.put(msg, shuffle_hist.get(msg) + 1);
						} else {
							shuffle_hist.put(msg, 1L);
						}
					} 
				}

			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				/* close child process */
				if(this.isTermination) {
					System.out.println("terminating...");
					if(serverSocket != null ) {
						System.out.println("socket close");
						serverSocket.close();
					}
					System.out.println("END");
				}
			}
		}
	}

	public String makeJsonString(long time, Map<String, Long> hist) {
		Map<String,Object> map = new HashMap<String,Object>();
		Map<String, Object> in_map = new HashMap<String, Object>(); 
		//fluentd v0.10.34以降では、
		// format jsonの場合timeが残せない？
		// http://qiita.com/rch850/items/3b7ce04e38c85a1ce5d0
		//map.put("time", this.now());
		map.put("timestamp", this.now());
		long sum = countAllFreq(hist);
		if(sum > 0) {
			map.put("keymap", hist);
		}
		in_map.put("sum", sum);
		map.put("metrics", in_map);
		ObjectMapper mapper = new ObjectMapper();
		String json = "{" + "timestamp" + ":" + this.now() + 
				"metrics:" + " {sum:" + sum +"}" + "}";

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

	public boolean terminate() {
		this.isTermination = true;
		return true;
	}
	
    /**
     * get timestamp
     * @args
     * @return String timestamp
     */
    public String now() {
        //long now = System.currentTimeMillis();
        //return Long.toString(now);
        Date date = new Date();
    	TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
        return dayFormat.format(date);
    }
}

