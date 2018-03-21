package infradoop.core.common.source;

import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import infradoop.core.common.data.StringDataConverter;
import infradoop.core.common.entity.Attribute;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;

public class Cdh5HbaseEntityWriter extends EntityWriter {
	private List<Put> puts;
	private Table table;
	private Put put;
	private boolean hasvalue[];
	
	public Cdh5HbaseEntityWriter(Connector connector, EntityDescriptor entityDescriptor, EntityWriterOptions options) {
		super(connector, entityDescriptor, options);
		if (entityDescriptor.getDomain() == null)
			throw new IllegalArgumentException("The entity domain can't not be null "
					+ "["+entityDescriptor.getCanonicalName()+", "+connector.getConnectorType()+", "+connector.getAccount().getName()+"]");
	}

	@Override
	public void initialize() throws IOException {
		Connection hbase = (Connection)connector.unwrap();
		table = hbase.getTable(TableName.valueOf(entityDescriptor.getDomain(), entityDescriptor.getName()));
		puts = new ArrayList<>(writerOptions.getBatchSize());
		hasvalue = new boolean[entityDescriptor.countAttributes()];
	}
	
	@Override
	public boolean hasValue(int index) throws IOException {
		return hasvalue[index];
	}
	@Override
	public Object getValue(int index) throws IOException {
		Attribute attr = getAttribute(index);
		if (index == 0)
			return getObject(attr, put.getRow());
		else {
			List<Cell> cells = put.get(attr.getFamilyAsByteArray(), attr.getNameAsByteArray());
			if (cells.size() == 0)
				return null;
			return getObject(attr, CellUtil.cloneValue(cells.get(cells.size()-1)));
		}
	}
	
	@Override
	public void setValue(int index, String value) throws IOException {
		Attribute attr = getAttribute(index);
		if (index == 0) {
			put = new Put(getBytes(attr, value));
		} else {
			put.addColumn(attr.getFamilyAsByteArray(), attr.getNameAsByteArray(),
					getBytes(attr, value));
		}
		hasvalue[index] = true;
	}
	
	@Override
	public void setValue(int index, Object value) throws IOException {
		Attribute attr = getAttribute(index);
		if (index == 0) {
			put = new Put(getBytes(attr, value));
		} else {
			put.addColumn(attr.getFamilyAsByteArray(), attr.getNameAsByteArray(),
					getBytes(attr, value));
		}
		hasvalue[index] = true;
	}
	
	@Override
	public void write() throws IOException {
		evaluateDynamicValues();
		if (puts.size() == writerOptions.getBatchSize()) {
			table.put(puts);
			puts.clear();
		}
		puts.add(put);
		Arrays.fill(hasvalue, false);
	}
	
	protected byte[] getBytes(Attribute attr, String value) throws IOException {
		if (value == null)
			return new byte[0];
		try {
			switch (attr.getType()) {
			case STRING:
				return Bytes.toBytes(value);
			case INT:
				return Bytes.toBytes(StringDataConverter.toInt(value));
			case BIGINT:
				return Bytes.toBytes(StringDataConverter.toLong(value));
			case FLOAT:
				return Bytes.toBytes(StringDataConverter.toFloat(value));
			case DOUBLE:
				return Bytes.toBytes(StringDataConverter.toDouble(value));
			case DATE:
			case TIMESTAMP:
				return Bytes.toBytes(StringDataConverter.toEpochTime(attr, value));
			case BOOLEAN:
				return Bytes.toBytes(StringDataConverter.toBoolean(value));
			case BINARY:
				return Bytes.toBytes(StringDataConverter.toBinary(value));
			default:
				throw new IllegalArgumentException("unable to processing value ["+value+"] "
						+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]");
			}
		} catch (ParseException e) {
			throw new IOException("unable to processing value ["+value+"] "
					+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]", e);
		}
	}
	
	protected byte[] getBytes(Attribute attr, Object value) throws IOException {
		if (value == null)
			return new byte[0];
		try {
			switch (attr.getType()) {
			case INT:
				return Bytes.toBytes((Integer)value);
			case BIGINT:
				return Bytes.toBytes((Long)value);
			case FLOAT:
				return Bytes.toBytes((Float)value);
			case DOUBLE:
				return Bytes.toBytes((Double)value);
			case DATE:
			case TIMESTAMP:
				return Bytes.toBytes(((Date)value).getTime());
			case BOOLEAN:
				return Bytes.toBytes((Boolean)value);
			case BINARY:
				return Bytes.toBytes(StringDataConverter.toBinary((byte[])value));
			default:
				throw new IllegalArgumentException("unable to processing value ["+value.toString()+"] "
						+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]");
			}
		} catch (ClassCastException e) {
			throw new IOException("unable to processing value ["+value.toString()+"] "
					+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]", e);
		}
	}
	
	protected Object getObject(Attribute attr, byte[] data) throws IOException {
		if (data == null)
			return null;
		if (data.length == 0)
			return null;
		switch (attr.getType()) {
		case INT:
			return Bytes.toInt(data);
		case BIGINT:
			return Bytes.toLong(data);
		case FLOAT:
			return Bytes.toFloat(data);
		case DOUBLE:
			return Bytes.toDouble(data);
		case DATE:
		case TIMESTAMP:
			return new Date(Bytes.toLong(data));
		case BINARY:
			try {
				return Hex.decodeHex(new String(data).toCharArray());
			} catch (DecoderException e) {
				throw new IOException("unable to processing value [BINARY] "
						+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]", e);
			}
		default:
			throw new IllegalArgumentException("unable to processing value [BINARY] "
					+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]");
		}
	}
	
	@Override
	public void uninitialize() throws IOException {
		if (!puts.isEmpty())
			table.put(puts);
	}
}
