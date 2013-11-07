package com.github.karahiyo.hanoi_picker;

import java.io.IOException;

public class HanoiPicker {

	public static void main(String[] args) throws IOException{
		PickerDaemon picker = new PickerDaemon();
		try {
			picker.setupSocket();
			picker.setupOutDir("/tmp");
			picker.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
