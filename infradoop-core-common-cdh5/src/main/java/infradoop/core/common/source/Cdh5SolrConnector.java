package infradoop.core.common.source;

import infradoop.core.common.entity.DataType;
import infradoop.core.common.SystemConfiguration;
import infradoop.core.common.account.Account;
import infradoop.core.common.entity.Attribute;
import infradoop.core.common.entity.Nameable;
import infradoop.core.common.entity.EntityDescriptor;
import infradoop.core.common.entity.EntityNameable;
import infradoop.core.common.entity.EntityWriter;
import infradoop.core.common.entity.EntityWriterOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.zookeeper.KeeperException;

public class Cdh5SolrConnector extends SolrConnector {
	private static final Logger LOG = Logger.getLogger(Cdh5SolrConnector.class);
	
	public Cdh5SolrConnector(Account account, SolrServer connection) {
		super(account, connection);
	}
	
	@Override
	public String[] getDomains() throws IOException {
		Set<String> domains = new HashSet<>();
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		ZkStateReader zkReader = solr.getZkStateReader();
		for (String collection : zkReader.getClusterState().getCollections()) {
			if (collection.contains("."))
				domains.add(collection.split("\\.", 2)[0]);
			else
				domains.add(null);
		}
		return domains.toArray(new String[domains.size()]);
	}
	
	@Override
	public String[] getEntities(String domain) throws IOException {
		List<String> entities = new ArrayList<>();
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		ZkStateReader zkReader = solr.getZkStateReader();
		for (String collection : zkReader.getClusterState().getCollections()) {
			if (domain == null && !collection.contains(".")) {
				entities.add(collection);
			} else if (collection.contains(".")) {
				String p[] = collection.split("\\.", 2);
				if (p[0].equals(domain))
					entities.add(p[1]);
			}
		}
		return entities.toArray(new String[entities.size()]);
	}
	
	private void cloneSolrZookeeperConfig(SolrZkClient zk,
			String baseSource, String baseTarget, String resource)
					throws KeeperException, InterruptedException {
		for (String i : zk.getChildren(baseSource+resource, null, true)) {
			String current = resource+"/"+i;
			String source = baseSource+current;
			String target = baseTarget+current;
			byte[] data = zk.getData(source, null, null, true);
			if (data == null) {
				if (!zk.exists(target, true))
					zk.makePath(target, true);
				cloneSolrZookeeperConfig(zk, baseSource, baseTarget, current);
			} else {
				if (zk.exists(target, true))
					zk.delete(target, 0, true);
				zk.makePath(target, data, true);
			}
		}
	}
	private void createSolrZookeeperConfig(SolrZkClient zk, String template, String target, byte xmlSchema[])
			throws KeeperException, InterruptedException {
		/*if (zk.exists("/solr/configs/"+target, true))
			throw new EntityException(
					"unable to create zookeeper configuration for solr collection "+target
					+" the zookeeper path already exists");*/
		cloneSolrZookeeperConfig(zk, "/solr/configs/"+template, "/solr/configs/"+target, "");
		if (zk.exists("/solr/configs/"+target+"/schema.xml", true))
			zk.delete("/solr/configs/"+target+"/schema.xml", 0, true);
		zk.makePath("/solr/configs/"+target+"/schema.xml", xmlSchema, true);
	}
	private String getTypeName(Attribute attribute) throws IOException {
		switch (attribute.getType()) {
		case STRING:
			return "string";
		case INT:
			return "int";
		case BIGINT:
			return "long";
		case BOOLEAN:
			return "boolean";
		case FLOAT:
			return "float";
		case DOUBLE:
			return "double";
		case DATE:
		case TIMESTAMP:
			return "date";
		default:
			throw new IOException("unsupported type "+attribute.getType().name()
					+" for entity "+attribute.getEntity().getCanonicalName());
		}
	}
	private DataType getType(String typeName) throws IOException {
		switch (typeName) {
		case "string":
			return DataType.STRING;
		case "int":
			return DataType.INT;
		case "long":
			return DataType.BIGINT;
		case "boolean":
			return DataType.BOOLEAN;
		case "float":
			return DataType.FLOAT;
		case "double":
			return DataType.DOUBLE;
		case "date":
			return DataType.TIMESTAMP;
		default:
			throw new IOException("unsupported type "+typeName);
		}
	}
	private void deleteSolrZookeeperConfig(SolrZkClient zk,
			String baseSource, String resource)
					throws KeeperException, InterruptedException {
		for (String i : zk.getChildren(baseSource+resource, null, true)) {
			String current = resource+"/"+i;
			String source = baseSource+current;
			byte[] data = zk.getData(source, null, null, true);
			if (data == null) {
				deleteSolrZookeeperConfig(zk, baseSource, current);
				zk.delete(source, 0, true);
			} else {
				zk.delete(source, 0, true);
			}
		}
	}
	private void dropSolrZookeperConfig(SolrZkClient zk, String target)
			throws InterruptedException, KeeperException {
		deleteSolrZookeeperConfig(zk, "/solr/configs/"+target, "");
		zk.delete("/solr/configs/"+target, 0, true);
	}
	
	@Override
	public void createEntity(EntityDescriptor entity) throws IOException {
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		if (entity.countAttributes() < 1)
			throw new IOException("unable to create entity "+entity.getCanonicalName()+" with empty attributes");
		
		try {
			CircularList<String> servers = new CircularList<>();
			try (SolrZkClient zk = new SolrZkClient(SystemConfiguration.getProperty(
					"solr.zookeeper.quorum", SystemConfiguration.getProperty("hbase.zookeeper.quorum")), 5000)) {
				List<String> children = zk.getChildren("/solr/live_nodes", null, false);
				for (String c : children) {
					if (c.contains("_solr"))
						c = c.replace("_solr", "");
					if (c.contains(":"))
						c = c.split("\\:",2)[0];
					servers.add(c);
				}
				if (servers.isEmpty())
					throw new IOException("unable to create solr collection ["+entity.getCanonicalName()+"]"
							+ ", there aren't available serviers");
				
				StringBuilder sb = new StringBuilder();
				sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
				sb.append("<schema name=\"").append(entity.getCanonicalName()).append("\" version=\"1.5\">\n");
				sb.append("   <fields>\n");
				String idName;
				if (entity.isDynamics()) {
					idName = "id";
					Attribute idAttr = entity.getAttribute(0);
					String idType = getTypeName(idAttr);
					sb.append("      <field name=\"id\" type=\"").append(idType).append("\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\" />\n");
					sb.append("      <field name=\"_root_\" type=\"string\" indexed=\"true\" stored=\"false\" />\n");
					sb.append("      <field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_s\" type=\"string\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_ss\" type=\"string\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_b\" type=\"boolean\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_bs\" type=\"boolean\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_i\" type=\"int\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_is\" type=\"int\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_l\" type=\"long\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_ls\" type=\"long\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_f\" type=\"float\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_fs\" type=\"float\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_d\" type=\"double\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_ds\" type=\"double\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_dt\" type=\"date\" indexed=\"true\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_dts\" type=\"date\" indexed=\"true\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_bi\" type=\"binary\" indexed=\"false\" required=\"false\" stored=\"true\" />\n");
					sb.append("      <dynamicField name=\"*_bis\" type=\"binary\" indexed=\"false\" required=\"false\" stored=\"true\" multiValued=\"true\" />\n");
				} else {
					Attribute idAttr = entity.getAttribute(0);
					idName = idAttr.getName();
					sb.append("      <field name=\"").append(idAttr.getName()).append("\" type=\"").append(getTypeName(idAttr))
							.append("\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\" />\n");
					sb.append("      <field name=\"_root_\" type=\"string\" indexed=\"true\" stored=\"false\" />\n");
					sb.append("      <field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\" />\n");
					for (int i=1;i<entity.countAttributes();i++) {
						Attribute attr = entity.getAttribute(i);
						sb.append("      <field name=\"").append(attr.getName()).append("\" type=\"").append(getTypeName(attr)).append("\" indexed=\"").append(Boolean.toString(attr.isIndexable())).append("\" required=\"").append(Boolean.toString(attr.isRequired())).append("\" stored=\"true\"/>");
					}
				}
				sb.append("   </fields>\n");
				sb.append("   <uniqueKey>").append(idName).append("</uniqueKey>\n");
				sb.append("   <types>\n");
				sb.append("      <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />\n");
				sb.append("      <fieldType name=\"boolean\" class=\"solr.BoolField\" sortMissingLast=\"true\" />\n");
				sb.append("      <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />\n");
				sb.append("      <fieldType name=\"long\" class=\"solr.TrieLongField\" precisionStep=\"0\" positionIncrementGap=\"0\" />\n");
				sb.append("      <fieldType name=\"float\" class=\"solr.TrieFloatField\" precisionStep=\"0\" positionIncrementGap=\"0\" />\n");
				sb.append("      <fieldType name=\"double\" class=\"solr.TrieDoubleField\" precisionStep=\"0\" positionIncrementGap=\"0\" />\n");
				sb.append("      <fieldType name=\"date\" class=\"solr.TrieDateField\" precisionStep=\"0\" positionIncrementGap=\"0\" />\n");
				sb.append("      <fieldtype name=\"binary\" class=\"solr.BinaryField\" />\n");
				sb.append("   </types>\n");
				sb.append("</schema>\n");
				
				createSolrZookeeperConfig(zk,
						System.getProperty("solr.collection.template", "predefinedTemplate"),
						entity.getCanonicalName(), sb.toString().getBytes());
			}
			
			CoreAdminRequest.Create createReq = new CoreAdminRequest.Create();
			createReq.setCollection(entity.getCanonicalName());
			createReq.setCollectionConfigName(entity.getCanonicalName());
			createReq.setNumShards(entity.getShards());
			
			ZkStateReader zkReader = solr.getZkStateReader();
			ClusterState clusterState = zkReader.getClusterState();
			CircularList<String> solrServers = new CircularList<>();
			for (String liveNode : clusterState.getLiveNodes()) {
				solrServers.add(zkReader.getBaseUrlForNodeName(liveNode));
			}
			
			for (int i=1;i<=entity.getShards();i++) {
				createReq.setShardId("shard"+i);
				createReq.setCoreName(entity.getCanonicalName()+"_shard"+i);
				String solrServer = solrServers.get(i-1);
				HttpSolrServer reqSolrServer;
				if (SystemConfiguration.isSecurityEnabled())
					reqSolrServer = new Cdh5KerberosHttpSolrServer(getAccount(), solrServer);
				else
					reqSolrServer = new Cdh5SimpleHttpSolrServer(getAccount(), solrServer);
				createReq.process(reqSolrServer);
				reqSolrServer.shutdown();
			}
		} catch (SolrServerException | KeeperException | InterruptedException e) {
			throw new IOException("unable to drop solr collection ["+entity.getCanonicalName()+"]");
		}
	}
	
	@Override
	public EntityDescriptor buildEntityDescriptor(String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(entity));
	}
	@Override
	public EntityDescriptor buildEntityDescriptor(String domain, String entity) throws IOException {
		return buildEntityDescriptor(new EntityNameable(domain, entity));
	}
	@SuppressWarnings("unchecked")
	@Override
	public EntityDescriptor buildEntityDescriptor(Nameable nameable) throws IOException {
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		solr.setDefaultCollection(nameable.getCanonicalName());
		EntityDescriptor entity = new EntityDescriptor(nameable);
		try {
			// recupera numero de shards
			DocCollection collection = solr.getZkStateReader().getClusterState()
					.getCollection(nameable.getCanonicalName());
			entity.setShards(collection.getSlices().size());
			// recupera atributos
			SolrQuery query = new SolrQuery();
			query.add(CommonParams.QT, "/schema/fields");
			QueryResponse response = solr.query(query);
			List<SimpleOrderedMap<?>> fields = (List<SimpleOrderedMap<?>>)response.getResponse().get("fields");
			if (fields != null && fields.size() > 0) {
				for (SimpleOrderedMap<?> field : fields) {
					String fieldName = (String)field.get("name");
					if (!"_root_".equals(fieldName) && !"_version_".equals(fieldName)) {
						entity.addAttribute(fieldName, getType((String)field.get("type")))
								.setRequired((Boolean)field.get("required"))
								.setIndexable((Boolean)field.get("indexed"));
					}
				}
			}
			// verificar si tiene campos dynamicos
			query = new SolrQuery();
			query.add(CommonParams.QT, "/schema/dynamicfields");
			response = solr.query(query);
			fields = (List<SimpleOrderedMap<?>>)response.getResponse().get("dynamicfields");
			if (fields != null && fields.size() > 0)
				entity.setDynamics(true);
			// retornar entidad
			return entity;
		} catch (SolrServerException e) {
			throw new IOException("unable to build solr collection ["+entity.getCanonicalName()+"]", e);
		}
	}
	@Override
	public void dropEntity(Nameable nameable) throws IOException {
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		try {
			CoreAdminRequest.Unload unloadReq = new CoreAdminRequest.Unload(true);
			unloadReq.setDeleteIndex(true);
			unloadReq.setDeleteInstanceDir(true);
			unloadReq.setDeleteDataDir(true);
			
			ZkStateReader zkReader = solr.getZkStateReader();
			DocCollection coll = zkReader.getClusterState().getCollection("default.test_solr");
			for (Slice s : coll.getSlices()) {
				for (Replica r : s.getReplicas()) {
					unloadReq.setCoreName(r.getProperties().get("core").toString());
					HttpSolrServer reqSolrServer;
					if (SystemConfiguration.isSecurityEnabled())
						reqSolrServer = new Cdh5KerberosHttpSolrServer(getAccount(),
								r.getProperties().get("base_url").toString());
					else
						reqSolrServer = new Cdh5SimpleHttpSolrServer(getAccount(),
								r.getProperties().get("base_url").toString());
					unloadReq.process(reqSolrServer);
					reqSolrServer.shutdown();
				}
			}
		} catch (SolrServerException e) {
			throw new IOException("unable to drop solr collection ["+nameable.getCanonicalName()+"]");
		} finally {
			try (SolrZkClient zk = new SolrZkClient(SystemConfiguration.getProperty(
						"solr.zookeeper.quorum", SystemConfiguration.getProperty("hbase.zookeeper.quorum")), 5000)) {
				dropSolrZookeperConfig(zk, nameable.getCanonicalName());
			} catch (InterruptedException | KeeperException e) {
				LOG.error("unable to drop zookeper configuration from solr collection ["+nameable.getCanonicalName()+"]", e);
			}
		}
	}
	@Override
	public boolean entityExists(Nameable nameable) throws IOException {
		CloudSolrServer solr = (CloudSolrServer)unwrap();
		try {
			ZkStateReader zkReader = solr.getZkStateReader();
			zkReader.getClusterState().getCollection(nameable.getCanonicalName());
			return true;
		} catch (SolrException e) {
			return false;
		}
	}
	
	@Override
	public EntityWriter getEntityWriter(EntityDescriptor entityDesc, EntityWriterOptions options) throws IOException {
		return new Cdh5SolrEntityWriter(this, entityDesc, options);
	}
	
	private static class CircularList<E> extends ArrayList<E> {
		public CircularList() {
			super();
		}
		@Override
		public E get(int index) {
			if (isEmpty()) {
				throw new IndexOutOfBoundsException("The list is empty");
			}
			while (index < 0) {
				index = size() + index;
			}
			return super.get(index % size());
		}
	}
	
	@Override
	public void setDefaultCollection(String collection) {
		((CloudSolrServer)connection).setDefaultCollection(collection);
	}
	@Override
	public UpdateResponse add(Collection<SolrInputDocument> docs) throws IOException {
		try {
			return ((SolrServer)connection).add(docs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs)
			throws IOException {
		try {
			return ((SolrServer)connection).add(docs, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse addBeans(Collection<?> beans) throws IOException {
		try {
			return ((SolrServer)connection).addBeans(beans);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).addBeans(beans, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse add(SolrInputDocument doc) throws IOException {
		try {
			return ((SolrServer)connection).add(doc);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).add(doc, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse addBean(Object obj) throws IOException {
		try {
			return ((SolrServer)connection).addBean(obj);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).addBean(obj, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse commit() throws IOException {
		try {
			return ((SolrServer)connection).commit();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse optimize() throws IOException {
		try {
			return ((SolrServer)connection).optimize();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws IOException {
		try {
			return ((SolrServer)connection).commit(waitFlush, waitSearcher);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit)
			throws IOException {
		try {
			return ((SolrServer)connection).commit(waitFlush, waitSearcher, softCommit);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws IOException {
		try {
			return ((SolrServer)connection).optimize(waitFlush, waitSearcher);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws IOException {
		try {
			return ((SolrServer)connection).optimize(waitFlush, waitSearcher, maxSegments);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse rollback() throws IOException {
		try {
			return ((SolrServer)connection).rollback();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteById(String id) throws IOException {
		try {
			return ((SolrServer)connection).deleteById(id);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteById(String id, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).deleteById(id, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteById(List<String> ids) throws IOException {
		try {
			return ((SolrServer)connection).deleteById(ids);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).deleteById(ids, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteByQuery(String query) throws IOException {
		try {
			return ((SolrServer)connection).deleteByQuery(query);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws IOException {
		try {
			return ((SolrServer)connection).deleteByQuery(query, commitWithinMs);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public SolrPingResponse ping() throws IOException {
		try {
			return ((SolrServer)connection).ping();
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public QueryResponse query(SolrParams params) throws IOException {
		try {
			return ((SolrServer)connection).query(params);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public QueryResponse query(SolrParams params, METHOD method) throws IOException {
		try {
			return ((SolrServer)connection).query(params, method);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	@Override
	public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback)
			throws IOException {
		try {
			return ((SolrServer)connection).queryAndStreamResponse(params, callback);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	
	@Override
	public void close() throws IOException {
		if (getPool() == null)
			((SolrServer)connection).shutdown();
		super.close();
	}
}
