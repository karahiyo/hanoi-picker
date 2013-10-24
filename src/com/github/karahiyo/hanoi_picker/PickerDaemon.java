package com.github.karahiyo.hanoi_picker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * receive message from PickerClient, and count-up keys.
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
	
	/** a flag for jedging receiving data from client now. */
    private boolean         isRecievedNow;

    /** a flag for receiving teminate message. */
    private boolean         isTermination;
    
    /** server socket timeout(ms) */
    public static final int    TIMEOUT_SERVER_SOCKET     = 500;
    
	public PickerDaemon() throws IOException {
		try {
			this.serverSocket = new ServerSocket(this.PORT);
			this.serverSocket.setSoTimeout(this.TIMEOUT_SERVER_SOCKET);
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	@Override
	public void run() {

		while ( ! this.isTermination) {
			Socket connectedSocket = null;
			
			try {
				connectedSocket = this.serverSocket.accept();
			} catch (SocketTimeoutException timeExc) {
				// nothing to do
			} catch (IOException e) {
				System.out.println(e);
			}
			
			// not accepting data
			if ( connectedSocket == null) {
				continue;
			}
	
			try {
				/* get data input stream of socket */
				is = new DataInputStream(connectedSocket.getInputStream());

				/* get outputstream of sub process */
				os  = new PrintStream(connectedSocket.getOutputStream());
				BufferedReader bf = new BufferedReader(new InputStreamReader(is));

				String line;
				while ( (line = bf.readLine()) != null) {
					System.out.println(line);
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
}
