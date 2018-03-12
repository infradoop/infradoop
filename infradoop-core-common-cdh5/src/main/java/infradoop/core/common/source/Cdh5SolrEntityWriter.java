package infradoop.core.common.source;

import infradoop.core.common.entity.Attribute;
import infradoop.core.common.StringDataConverter;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class Cdh5SolrEntityWriter extends EntityWriter {
	private List<SolrInputDocument> docs;
	private SolrInputDocument currentDoc;
	
	public Cdh5SolrEntityWriter(Connector connector, EntityDescriptor entity,
			EntityWriterOptions options) {
		super(connector, entity, options);
	}

	@Override
	public void initialize() throws IOException {
		docs = new ArrayList<>(options.getBatchSize());
		currentDoc = new SolrInputDocument();
		((CloudSolrServer)connector.unwrap())
			.setDefaultCollection(entity.getCanonicalName());
	}
	
	@Override
	public EntityWriter set(int index, String value) throws IOException {
		Attribute attr = entity.getAttribute(index);
		if (value == null)
			currentDoc.setField(attr.getName(), null);
		else
			try {
				switch (attr.getType()) {
				case STRING:
					currentDoc.setField(attr.getName(), value);
					break;
				case INT:
					currentDoc.setField(attr.getName(), StringDataConverter.toInt(value));
					break;
				case BIGINT:
					currentDoc.setField(attr.getName(), StringDataConverter.toLong(value));
					break;
				case FLOAT:
					currentDoc.setField(attr.getName(), StringDataConverter.toFloat(value));
					break;
				case DOUBLE:
					currentDoc.setField(attr.getName(), StringDataConverter.toDouble(value));
					break;
				case DATE:
				case TIMESTAMP:
					currentDoc.setField(attr.getName(), StringDataConverter.toDouble(value));					
					break;
				case BOOLEAN:
					currentDoc.setField(attr.getName(), StringDataConverter.toBoolean(value));					
					break;
				case BINARY:
					currentDoc.setField(attr.getName(), StringDataConverter.toBinary(value));					
					break;
				default:
					throw new IllegalArgumentIOException("unable to processing value ["+value+"] "
							+ "["+entity.getCanonicalName()+", "+attr.getName()+"]");
				}
			} catch (ParseException e) {
				throw new IOException("unable to processing value ["+value+"] "
						+ "["+entity.getCanonicalName()+", "+attr.getName()+"]", e);
			}
		return this;
	}
	@Override
	public EntityWriter set(int index, Object value) throws IOException {
		currentDoc.addField(entity.getAttribute(index).getName(), value);
		return this;
	}
	@Override
	public EntityWriter write() throws IOException {
		if (docs.size() == options.getBatchSize())
			addToSolr();
		docs.add(currentDoc);
		currentDoc = new SolrInputDocument();
		return this;
	}
	
	private void addToSolr() throws IOException {
		((SolrConnector)connector).add(docs);
		((SolrConnector)connector).commit();
		docs.clear();
	}
	
	@Override
	public void uninitialize() throws IOException {
		if (!docs.isEmpty())
			addToSolr();
	}
}
