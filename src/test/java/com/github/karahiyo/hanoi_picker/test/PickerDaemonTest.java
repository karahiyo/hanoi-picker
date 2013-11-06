package com.github.karahiyo.hanoi_picker.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.github.karahiyo.hanoi_picker.PickerDaemon;

public class PickerDaemonTest extends TestCase{

	public PickerDaemonTest(String name){
		super(name);
	}

	@Test
	public void testMakeJsonString() {
		PickerDaemon picker = new PickerDaemon();
		long time = 1383115449245L;
		Map<String, Long> hist = new HashMap<String, Long>();
		hist.put("a", 1L);
		hist.put("b", 3L);
		hist.put("c", 19L);
		
		String json = picker.makeJsonString(time, hist) ;
		// make Jackson mapper object
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> result = new HashMap<String, Object>();
		try {
			result = mapper.readValue(json, Map.class);
		} catch (IOException e) {
			System.err.println(e.getStackTrace());
			fail("Error: in read json string into Json ObjectMapper");
		}
		assertEquals((long)result.get("time"), 1383115449245L);
		Map<String,Long> keymap = (HashMap<String, Long>)result.get("keymap");

		//"{\"time\":1383115449245,\"keymap\":{\"a\":1,\"b\":3,\"c\",19},\"sum\":23}"
		System.err.println(keymap.get("a"));
		System.err.println((Long)keymap.get("b"));
		assertEquals(1L, (long)keymap.get("a"));
		assertEquals("3", keymap.get("b"));
		assertEquals("19", keymap.get("c"));
		assertEquals("23", result.get("sum"));
		
	}
	
	@Test
	public void testCountAllFreq() {
		PickerDaemon picker = new PickerDaemon();
		Map<String, Long> map = new HashMap<String,Long>();
		map.put("a", 10L);
		map.put("b", 7L);
		map.put("c", 178L);
		long sum = picker.countAllFreq(map);
		assertEquals(sum, 195L);
	}
	
	@Test
	public void testCountAllFreqLong() {
		PickerDaemon picker = new PickerDaemon();
		Map<String, Long> map = new HashMap<String,Long>();
		map.put("a", 1000000L);
		map.put("b", 7000000L);
		map.put("c", 178000000L);
		long sum = picker.countAllFreq(map);
		assertEquals(sum, 195000000L);
	}

}
