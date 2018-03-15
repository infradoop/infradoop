package infradoop.core.common.source;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HttpContext;

import infradoop.core.common.account.Account;

public class HttpClientConnector extends AbstractConnector {
	public HttpClientConnector(Account account, Object connection) {
		super(account, connection);
	}
	
	@Override
	public String getConnectorType() {
		return "http";
	}
	
	public HttpResponse execute(final HttpUriRequest hur) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<HttpResponse>() {
				@Override
				public HttpResponse run() throws Exception {
					return ((HttpClient)connection).execute(hur);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public HttpResponse execute(final HttpUriRequest hur, final HttpContext hc) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<HttpResponse>() {
				@Override
				public HttpResponse run() throws Exception {
					return ((HttpClient)connection).execute(hur, hc);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public HttpResponse execute(final HttpHost hh, final HttpRequest hr) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<HttpResponse>() {
				@Override
				public HttpResponse run() throws Exception {
					return ((HttpClient)connection).execute(hh, hr);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}		
	}
	public HttpResponse execute(final HttpHost hh, final HttpRequest hr, final HttpContext hc) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<HttpResponse>() {
				@Override
				public HttpResponse run() throws Exception {
					return ((HttpClient)connection).execute(hh, hr, hc);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public <T> T execute(final HttpUriRequest hur, final ResponseHandler<? extends T> rh) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<T>() {
				@Override
				public T run() throws Exception {
					return ((HttpClient)connection).execute(hur, rh);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public <T> T execute(final HttpUriRequest hur, final ResponseHandler<? extends T> rh, final HttpContext hc) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<T>() {
				@Override
				public T run() throws Exception {
					return ((HttpClient)connection).execute(hur, rh, hc);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public <T> T execute(final HttpHost hh, final HttpRequest hr, final ResponseHandler<? extends T> rh) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<T>() {
				@Override
				public T run() throws Exception {
					return ((HttpClient)connection).execute(hh, hr, rh);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	public <T> T execute(final HttpHost hh, final HttpRequest hr, final ResponseHandler<? extends T> rh, final HttpContext hc) throws IOException, ClientProtocolException {
		try {
			return account.getUserGroupInformation().doAs(new PrivilegedExceptionAction<T>() {
				@Override
				public T run() throws Exception {
					return ((HttpClient)connection).execute(hh, hr, rh, hc);
				}
			});
		} catch (InterruptedException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	
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
