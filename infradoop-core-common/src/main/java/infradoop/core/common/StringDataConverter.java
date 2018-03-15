package infradoop.core.common;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.codec.binary.Hex;

import infradoop.core.common.entity.Attribute;

public class StringDataConverter {
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	
	public static int toInt(String value) throws ParseException {
		return NUMBER_FORMAT.parse(value).intValue();
	}
	public static long toLong(String value) throws ParseException {
		return NUMBER_FORMAT.parse(value).longValue();
	}
	public static float toFloat(String value) throws ParseException {
		return NUMBER_FORMAT.parse(value).floatValue();
	}
	public static double toDouble(String value) throws ParseException {
		return NUMBER_FORMAT.parse(value).doubleValue();
	}
	public static long toEpochTime(Attribute attr, String value) throws ParseException {
		return (long)(attr.getDateFormat().parse(value).getTime()/1000L);
	}
	public static Date toTime(Attribute attr, String value) throws ParseException {
		return attr.getDateFormat().parse(value);
	}
	public static boolean toBoolean(String value) {
		return "true".equalsIgnoreCase(value);
	}
	public static String toBinary(String value) {
		return Hex.encodeHexString(value.getBytes());
	}
	public static String toBinary(byte value[]) {
		return Hex.encodeHexString(value);
	}
}
