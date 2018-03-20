package infradoop.core.common.data;

import org.apache.commons.lang.StringUtils;

public class VariableParser {
	public static String parse(String data, VariableResolver resolver) {
		if (data == null)
			return null;
		StringBuilder sb = new StringBuilder(data);
		int offset = 0;
		while ((offset = sb.indexOf("${", offset)) >= 0) {
			int endOffset = sb.indexOf("}", offset);
			if (endOffset >= 0) {
				String var = sb.substring(offset+2, endOffset);
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
				offset+=2;
			}
		}
		return sb.toString();
	}
}
