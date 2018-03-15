package infradoop.core.common.source;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

import infradoop.core.common.account.Account;

public abstract class SolrConnector extends AbstractConnectorEntityHandler {
	public SolrConnector(Account account, Object connection) {
		super(account, connection);
	}

	@Override
	public String getConnectorType() {
		return "solr";
	}
	
	public abstract void setDefaultCollection(String collection);
	public abstract UpdateResponse add(Collection<SolrInputDocument> docs) throws IOException;
	public abstract UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws IOException;
	public abstract UpdateResponse addBeans(Collection<?> beans) throws IOException;
	public abstract UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws IOException;
	public abstract UpdateResponse add(SolrInputDocument doc) throws IOException;
	public abstract UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws IOException;
	public abstract UpdateResponse addBean(Object obj) throws IOException;
	public abstract UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException;
	public abstract UpdateResponse commit() throws IOException;
	public abstract UpdateResponse optimize() throws IOException;
	public abstract UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws IOException;
	public abstract UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws IOException;
	public abstract UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws IOException;
	public abstract UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws IOException;
	public abstract UpdateResponse rollback() throws IOException;
	public abstract UpdateResponse deleteById(String id) throws IOException;
	public abstract UpdateResponse deleteById(String id, int commitWithinMs) throws IOException;
	public abstract UpdateResponse deleteById(List<String> ids) throws IOException;
	public abstract UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws IOException;
	public abstract UpdateResponse deleteByQuery(String query) throws IOException;
	public abstract UpdateResponse deleteByQuery(String query, int commitWithinMs) throws IOException;
	public abstract SolrPingResponse ping() throws IOException;
	public abstract QueryResponse query(SolrParams params) throws IOException;
	public abstract QueryResponse query(SolrParams params, METHOD method) throws IOException;
	public abstract QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback) throws IOException;
	
	@Override
	public void close() throws IOException {
		try {
			super.close();
		} catch (Exception e) {
			throw new IOException("unable to close connector "
					+ "["+getConnectorType()+", "+getAccount().getName()+"]", e);
		}
	}
}
