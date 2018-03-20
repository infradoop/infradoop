package infradoop.core.common.data;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import infradoop.core.common.SystemConfiguration;

public class SystemDefinedVariables implements DefinedVariables {
	private final Map<String, String> map;
	
	public SystemDefinedVariables() {
		map = new HashMap<>();
		map.put("currentEpoch", Long.toString(System.currentTimeMillis()));
		try {
			map.put("currentHost", InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			map.put("currentHost", "localhost");
		}
		map.put("currentUser", System.getProperty("user.name"));
		map.put("currentUserHome", System.getProperty("user.home"));
		SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		map.put("currentDate", sd.format(new Date()));
		
		for (Entry<String, String> entry : SystemConfiguration.getConfiguration())
			map.put(entry.getKey(), entry.getValue());
	}
	
	public void set(String key, String value) {
		map.put(key, value);
	}
	
	@Override
	public String get(String key) {
		return map.get(key);
	}

	@Override
	public boolean containsKey(String key) {
		return map.containsKey(key);
	}
}
