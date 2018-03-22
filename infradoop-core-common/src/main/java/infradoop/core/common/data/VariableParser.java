package infradoop.core.common.data;

import org.apache.commons.lang.StringUtils;

public class VariableParser {
	private static final String START_EXPRESSION = "$"+"{";
	private static final String END_EXPRESSION = "}";
	
	public static String parse(String data, VariableResolver resolver) {
		if (data == null)
			return null;
		StringBuilder sb = new StringBuilder(data);
		int offset = 0;
		while ((offset = sb.indexOf(START_EXPRESSION, offset)) >= 0) {
			int endOffset = sb.indexOf(END_EXPRESSION, offset);
			if (endOffset >= 0) {
				String var = sb.substring(offset+START_EXPRESSION.length(), endOffset);
				String params[];
				if (var.contains(",")) {
					String vararr[] = StringUtils.split(var, ",");
					var = vararr[0];
					params = new String[vararr.length-1];
					System.arraycopy(vararr, 1, params, 0, params.length-1);
				} else {
					params = null;
				}
				sb.replace(offset, endOffset+1, resolver.resolve(var, params));
			} else {
				offset+=START_EXPRESSION.length();
			}
		}
		return sb.toString();
	}
}
