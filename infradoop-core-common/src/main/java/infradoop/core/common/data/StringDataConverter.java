package infradoop.core.common.data;

import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.binary.Hex;

import infradoop.core.common.entity.Attribute;

public class StringDataConverter {
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
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
	public static Date toDate(Attribute attr, String value) throws ParseException {
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
	
	public static String toString(Object value) {
		if (value == null)
			return null;
		if (value instanceof Date) {
			return DATE_FORMAT.format((Date)value);
		} else {
			return value.toString();
		}
	}
	
	public static Object toObject(Attribute attr, String value) throws ParseException {
		switch (attr.getType()) {
		case STRING:
			return value;
		case INT:
			return StringDataConverter.toInt(value);
		case BIGINT:
			return StringDataConverter.toLong(value);
		case FLOAT:
			return StringDataConverter.toFloat(value);
		case DOUBLE:
			return StringDataConverter.toDouble(value);
		case DATE:
		case TIMESTAMP:
			return StringDataConverter.toDate(attr, value);
		case BOOLEAN:
			return StringDataConverter.toBoolean(value);
		case BINARY:
			return StringDataConverter.toBinary(value);
		default:
			throw new IllegalArgumentException("unable to processing value ["+value+"] "
					+ "["+attr.getEntity().getCanonicalName()+", "+attr.getName()+"]");
		}
	}
}
