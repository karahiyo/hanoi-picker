package com.github.karahiyo.hanoi_picker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.File;

public class PickerDaemon {

	private InputStream in = null;
	private InputStream ein = null;
	private OutputStream out = null;
	private Process process = null;

	public PickerDaemon() {
	}

	public void execCmd() throws InterruptedException, IOException {

		/* current working directory */
		File dir = new File("/tmp");
		
		/* exec command */
		String[] cmd = {"java","-version"};
		process = Runtime.getRuntime().exec(cmd, null, dir);

		/* get inputstream of sub process */
		in = process.getInputStream();

		/* get err inputstream of sub process */
		ein = process.getErrorStream();

		/* get outputstream of sub process */
		out  = process.getOutputStream();

		try {

			Runnable inputStreamThread = new Runnable(){
				public void run(){		
					try {
						System.out.println("** Thread stdRun start");
						BufferedReader bf = new BufferedReader(new InputStreamReader(in));
						String line;
						while ( (line = bf.readLine()) != null) {
							System.out.println(line);
						}
						System.out.println("** Thread stdRun end");
					} catch (Exception e) {		
						e.printStackTrace();      	
					}
				}
			};
			
			Runnable errStreamThread = new Runnable() {
				public void run() {
					try {
						System.out.println("** Thread errRun start");
						BufferedReader ebf = new BufferedReader(new InputStreamReader(ein));
						String errLine;
						while ( (errLine = ebf.readLine()) != null) {
							System.out.println(errLine);
						}
						System.out.println("** Thread errRun end");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			
			Thread stdRun = new Thread(inputStreamThread);
			Thread errRun = new Thread(errStreamThread);
			
			/* process start */
			stdRun.start();
			errRun.start();
			
			/* wait for process exit */
			int c = process.waitFor();
			
			/* wait for sub thread exit */
			stdRun.join();
			errRun.join();
			
			/* exit status code */
			System.out.println("** exit status: " + c);
		
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			/* close child process */
			if (in != null) in.close();
			if (ein != null) ein.close();
			if (out != null) out.close();
			process.destroy();
		}
	}
}
