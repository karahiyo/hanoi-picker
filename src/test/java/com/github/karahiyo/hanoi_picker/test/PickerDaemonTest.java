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
	public void testMakeJsonString() throws IOException{
		PickerDaemon picker = new PickerDaemon();
		long time = 1383115449245L;
		Map<String, Integer> hist = new HashMap<String, Integer>();
		hist.put("a", 1);
		hist.put("b", 3);
		hist.put("c", 19);
		
		String json = picker.makeJsonString(time, hist) ;
		// make Jackson mapper object
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> result = mapper.readValue(json, Map.class);
		assertEquals(result.get("time"), 1383115449245L);
		Map<String,Integer> keymap = (HashMap<String, Integer>)result.get("keymap");

		//"{\"time\":1383115449245,\"keymap\":{\"a\":1,\"b\":3,\"c\",19},\"sum\":23}"
		assertEquals((int)keymap.get("a"), 1);
		assertEquals((int)keymap.get("b"), 3);
		assertEquals((int)keymap.get("c"), 19);
		assertEquals((int)result.get("sum"), 23);
		
	}

}
