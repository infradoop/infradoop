package infradoop.core.common.data;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DefaultDefinedVariables implements DefinedVariables {
	private static final SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final String DEFAULT_HOSTNAME;
	private static final String DEFAULT_USERNAME;
	private static final String DEFAULT_USERHOME;
	
	static {
		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			host = "localhost";
		}
		DEFAULT_HOSTNAME = host;
		DEFAULT_USERNAME = System.getProperty("user.name");
		DEFAULT_USERHOME = System.getProperty("user.home");
	}
	
	private final Map<String, String> map;
	
	public DefaultDefinedVariables() {
		this(new HashMap<String, String>());
	}
	public DefaultDefinedVariables(Map<String, String> map) {
		this.map = map;
		
		this.map.put("current_hostname", DEFAULT_HOSTNAME);
		this.map.put("current_user_name", DEFAULT_USERNAME);
		this.map.put("current_user_home", DEFAULT_USERHOME);
		
		resetChangingVariables();
	}
	@Override
	public String get(String key) {
		return map.get(key);
	}
	@Override
	public void set(String key, String value) {
		map.put(key, value);
	}

	@Override
	public boolean containsKey(String key) {
		return map.containsKey(key);
	}

	public void resetChangingVariables() {
		this.map.put("current_epochtime", Long.toString(System.currentTimeMillis()));
		this.map.put("current_date", DEFAULT_FORMAT.format(new Date()));
	}
}
