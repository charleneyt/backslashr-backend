package webserver;

import java.util.*;

import webserver.Server.Worker;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
	String method;
	String url;
	String protocol;
	InetSocketAddress remoteAddr;
	Map<String, String> headers;
	Map<String, String> queryParams;
	Map<String, String> params;
	byte bodyRaw[];
	Server server;
	Worker worker;
	SessionImpl session = null;

	RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg,
			Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg,
			byte bodyRawArg[], Server serverArg) {
		method = methodArg;
		url = urlArg;
		remoteAddr = remoteAddrArg;
		protocol = protocolArg;
		headers = headersArg;
		queryParams = queryParamsArg;
		params = paramsArg;
		bodyRaw = bodyRawArg;
		server = serverArg;
	}

	public String requestMethod() {
		return method;
	}

	public int port() {
		return remoteAddr.getPort();
	}

	public String url() {
		return url;
	}

	public String protocol() {
		return protocol;
	}

	public String contentType() {
		return headers.get("content-type");
	}

	public String ip() {
		return remoteAddr.getAddress().getHostAddress();
	}

	public String body() {
		return new String(bodyRaw, StandardCharsets.UTF_8);
	}

	public byte[] bodyAsBytes() {
		return bodyRaw;
	}

	public int contentLength() {
		return bodyRaw.length;
	}

	public String headers(String name) {
		return headers.get(name.toLowerCase());
	}

	public Set<String> headers() {
		return headers.keySet();
	}

	public String queryParams(String param) {
		if (queryParams == null)
			return null;
		return queryParams.get(param);
	}

	public Set<String> queryParams() {
		if (queryParams == null)
			return null;
		return queryParams.keySet();
	}

	public String params(String param) {
		return params.get(param);
	}

	public Map<String, String> params() {
		return params;
	}

	public void worker(Worker worker) {
		this.worker = worker;
	}

	public void setSession(SessionImpl session) {
		this.session = session;
	}

	// When this method is first called for a given request, your server should
	// check whether the request included a cookie with the name SessionID and the
	// value of that cookie is currently associated with a Session object.
	// If such an object is found, the method should return it;
	// otherwise, it should
	// 1) pick a fresh, random session ID of at least 120 bits,
	// 2) instantiate a new Session object,
	// 3) associate this object with the chosen session ID,
	// 4) add a Set-Cookie header to the response that sets the SessionID cookie to
	// the chosen session ID, and
	// 5) return the Session object.
	// If the method is called again while the server is still handling the same
	// request, it should return the same Session object.
	// When the method is never called, no session objects or SessionID cookies
	// should be created.
	@Override
	public Session session() {
		if (session != null) {
			session.invalidate();
			if (session.valid) {
				session.setLastAccessedTime(System.currentTimeMillis());
				return session;
			}
		}

		worker.newSession = true;
		// generate sessionId
		String sessionId;
		while (Server.sessions.containsKey((sessionId = UUID.randomUUID().toString())))
			;
		session = new SessionImpl(sessionId);
		worker.currSession = session;
		// adds support for attributes on Set-Cookie header
		if (server.instanceSecureType) {
			worker.response.header("Set-Cookie", "SessionID=" + session.id() + "; Secure; HttpOnly; SameSite=Lax");
		} else {
			worker.response.header("Set-Cookie", "SessionID=" + session.id() + "; HttpOnly; SameSite=Lax");
		}

		Server.sessions.put(session.id(), session);

		return session;
	}
}
