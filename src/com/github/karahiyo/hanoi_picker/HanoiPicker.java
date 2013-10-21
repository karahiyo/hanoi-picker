package com.github.karahiyo.hanoi_picker;

import java.io.IOException;

public class HanoiPicker {

	public static void main(String[] args)  {
		PickerDaemon picker = new PickerDaemon();
		try {
			picker.execCmd();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}