package com.github.karahiyo.hanoi_picker;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
import org.fluentd.logger.FluentLogger;


/**
 * receive message from PickerClient, and count-up each keys.
 * 
 * @author yu_ke
 *
 */
public class PickerDaemon implements Runnable {

	/** get host name */
	private final static String HOST = getHostName();

	/** server socket */
	private DatagramSocket serverSocket;

	/** messsage send host */
	private SocketAddress socketAddress;

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
	public static final int HIST_OUT_INTETRVAL = 500; //ms

	/** map histogram data */
	public Map<String, Long> map_hist = new HashMap<String, Long>();

	/** reduce histogram data */
	public Map<String, Long> shuffle_hist = new HashMap<String, Long>();

    public static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

    // setup fluent-logger
    private static FluentLogger logger = FluentLogger.getLogger("hanoi.keymap.trace");
   
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


			/**
			 *  ヒストマップの出力 
			 */
			if(LAST_HIST_OUT_TIME + HIST_OUT_INTETRVAL <= System.currentTimeMillis() ) {
				Map<String, Object> data = new HashMap<String, Object>();
				data.put("key_map", map_hist.toString());
				data.put("key_types", map_hist.size());
				data.put("sum", countAllFreq(map_hist));
				logger.log("map." + HOST, data);
				map_hist.clear();
				data.clear();
				data.put("key_map", shuffle_hist);
				data.put("key_types", shuffle_hist.size());
				data.put("sum", countAllFreq(shuffle_hist));
				logger.log("shuffle." + HOST, data);
				shuffle_hist.clear();
				data.clear();

				/** update */
				LAST_HIST_OUT_TIME += HIST_OUT_INTETRVAL;
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

	public static String makeJsonString(long time, Map<String, Long> hist) {
		Map<String,Object> map = new HashMap<String,Object>();
		Map<String, Object> in_map = new HashMap<String, Object>(); 
		//fluentd v0.10.34以降では、
		// format jsonの場合timeが残せない？
		// http://qiita.com/rch850/items/3b7ce04e38c85a1ce5d0
		//map.put("time", this.now());
		map.put("timestamp", now());
		long sum = countAllFreq(hist);
		if(sum > 0) {
			map.put("keymap", hist);
		}
		in_map.put("sum", sum);
		map.put("metrics", in_map);
		ObjectMapper mapper = new ObjectMapper();
		String json = "{" + "timestamp" + ":" + now() + 
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

	public static long countAllFreq(Map<String, Long> map) {
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
    public static String now() {
        //long now = System.currentTimeMillis();
        //return Long.toString(now);
        Date date = new Date();
    	TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
        return dayFormat.format(date);
    }
    
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return "UnknownHost";
    }
}

