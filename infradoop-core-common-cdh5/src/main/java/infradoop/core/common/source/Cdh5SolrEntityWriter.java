package infradoop.core.common.source;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import infradoop.core.common.data.StringDataConverter;
import infradoop.core.common.entity.Attribute;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;

public class Cdh5SolrEntityWriter extends EntityWriter {
	private List<SolrInputDocument> docs;
	private SolrInputDocument currentDoc;
	
	public Cdh5SolrEntityWriter(Connector connector, EntityDescriptor entity,
			EntityWriterOptions options) {
		super(connector, entity, options);
	}

	@Override
	public void initialize() throws IOException {
		docs = new ArrayList<>(writerOptions.getBatchSize());
		currentDoc = new SolrInputDocument();
		((CloudSolrServer)connector.unwrap())
			.setDefaultCollection(entityDescriptor.getCanonicalName());
		
		for (int i = 0; i < entityDescriptor.countAttributes(); i++) {
			Attribute attr = getAttribute(i);
			String fieldName;
			if (entityDescriptor.useDynamics()) {
				if (i == 0) {
					fieldName = "id";
				} else {
					switch (attr.getType()) {
					case STRING:
					case BINARY:
						fieldName = attr.getName() + "_s";
						break;
					case INT:
						fieldName = attr.getName() + "_i";
						break;
					case BIGINT:
						fieldName = attr.getName() + "_l";
						break;
					case FLOAT:
						fieldName = attr.getName() + "_f";
						break;
					case DOUBLE:
						fieldName = attr.getName() + "_d";
						break;
					case BOOLEAN:
						fieldName = attr.getName() + "_b";
						break;
					case DATE:
					case TIMESTAMP:
						fieldName = attr.getName() + "_dt";
						break;
					default:
						throw new IOException(
								"data type unsupported " + attr.getType().name() + " [" + attr.toString() + "]");
					}
				}
			} else {
				fieldName = attr.getName();
			}
			attr.setFinalName(fieldName);
		}
	}
	
	@Override
	public Object getValue(int index) throws IOException {
		Attribute attr = getAttribute(index);
		return currentDoc.getFieldValue(attr.getFinalName());
	}
	@Override
	public void setValue(int index, String value) throws IOException {
		Attribute attr = getAttribute(index);
		if (value == null)
			currentDoc.setField(attr.getFinalName(), null);
		else
			try {
				currentDoc.setField(attr.getFinalName(), StringDataConverter.toObject(attr, value));
			} catch (ParseException e) {
				throw new IOException("unable to processing value ["+value+"] "
						+ "["+entityDescriptor.getCanonicalName()+", "+attr.getName()+"]", e);
			}
	}
	@Override
	public void setValue(int index, Object value) throws IOException {
		Attribute attr = getAttribute(index);
		currentDoc.addField(attr.getFinalName(), value);
	}
	@Override
	public void write() throws IOException {
		evaluateDynamicValues();
		if (docs.size() == writerOptions.getBatchSize())
			addToSolr();
		docs.add(currentDoc);
		currentDoc = new SolrInputDocument();
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
