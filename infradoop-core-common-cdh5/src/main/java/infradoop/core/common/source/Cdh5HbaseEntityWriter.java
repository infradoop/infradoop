package infradoop.core.common.source;

import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import infradoop.core.common.StringDataConverter;
import infradoop.core.common.entity.Attribute;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;

public class Cdh5HbaseEntityWriter extends EntityWriter {
	private Table table;
	private Put put;
	
	public Cdh5HbaseEntityWriter(Connector connector, EntityDescriptor entity, EntityWriterOptions options) {
		super(connector, entity, options);
		if (entity.getDomain() == null)
			throw new IllegalArgumentException("The entity domain can't not be null "
					+ "["+entity.getCanonicalName()+", "+connector.getConnectorType()+", "+connector.getAccount().getName()+"]");
	}

	@Override
	public void initialize() throws IOException {
		Connection hbase = (Connection)connector.unwrap();
		table = hbase.getTable(TableName.valueOf(entity.getDomain(), entity.getName()));
	}
	
	@Override
	public EntityWriter set(int index, String value) throws IOException {
		Attribute attr = entity.getAttribute(index);
		if (index == 0) {
			put = new Put(getBytes(attr, value));
		} else {
			put.addColumn(attr.getFamilyAsByteArray(), attr.getNameAsByteArray(),
					getBytes(attr, value));
		}
		return this;
	}
	
	@Override
	public EntityWriter set(int index, Object value) throws IOException {
		Attribute attr = entity.getAttribute(index);
		if (index == 0) {
			put = new Put(getBytes(attr, value));
		} else {
			put.addColumn(attr.getFamilyAsByteArray(), attr.getNameAsByteArray(),
					getBytes(attr, value));
		}
		return this;
	}
	
	@Override
	public EntityWriter write() throws IOException {
		table.put(put);
		return this;
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
						+ "["+entity.getCanonicalName()+", "+attr.getName()+"]");
			}
		} catch (ParseException e) {
			throw new IOException("unable to processing value ["+value+"] "
					+ "["+entity.getCanonicalName()+", "+attr.getName()+"]", e);
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
						+ "["+entity.getCanonicalName()+", "+attr.getName()+"]");
			}
		} catch (ClassCastException e) {
			throw new IOException("unable to processing value ["+value.toString()+"] "
					+ "["+entity.getCanonicalName()+", "+attr.getName()+"]", e);
		}
	}
	
	@Override
	public void uninitialize() throws IOException {
	}
}
