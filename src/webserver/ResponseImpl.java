package webserver;

import java.util.*;

import webserver.Server.*;
import exceptions.*;

public class ResponseImpl implements Response {
	String method;
	String url;
	String protocol;
	int statusCode = 200;
	String reasonPhrase = "OK";
	Map<String, Set<String>> headers;
	String contentType;
	byte bodyRaw[] = null;
	Server server;
	Worker worker;
	boolean before = true;
	boolean write = false;

	ResponseImpl(String methodArg, String urlArg, String protocolArg, Server serverArg) {
		method = methodArg;
		url = urlArg;
		protocol = protocolArg;
		server = serverArg;
		headers = new HashMap<>();
	}

	public void worker(Worker worker) {
		this.worker = worker;
	}

	@Override
	public void body(String body) {
		if (!write) {
			bodyRaw = body.getBytes();
		}
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (!write) {
			bodyRaw = bodyArg;
		}
	}

	@Override
	public void header(String name, String value) {
		if (!write) {
			if (headers.containsKey(name)) {
				headers.get(name).add(value);
			} else {
				headers.put(name, new HashSet<String>(Arrays.asList(value)));
			}
		}
	}

	@Override
	public void type(String contentType) {
		if (!write) {
			this.contentType = contentType;
			header("Content-Type", contentType);
		}
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		if (!write) {
			this.statusCode = statusCode;
			this.reasonPhrase = reasonPhrase;
		}
	}

	@Override
	public void write(byte[] b) throws Exception {
		if (!write) {
			// write out header the first time write() is called
			header("Connection", "close");
			// Ed post #166
			if (!headers.containsKey("Content-Type")) {
				header("Content-Type", "application/octet-stream");
			}
			worker.sendDynamicResponse(true);
			write = true;
		}

		worker.bw.write(b);
		worker.bw.flush();
	}

	@Override
	public void redirect(String U, int c) throws RedirectException {
		if (!write) {
			switch (c) {
			case 301:
				reasonPhrase = "Moved Permanently";
				break;
			case 302:
				reasonPhrase = "Found";
				break;
			case 303:
				reasonPhrase = "See Other";
				break;
			case 307:
				reasonPhrase = "Temporary Redirect";
				break;
			case 308:
				reasonPhrase = "Permanent Redirect";
				break;
			default:
				c = 301;
				reasonPhrase = "Moved Permanently";
				break;
			}
			header("Location", U);
			status(c, reasonPhrase);
			body(U);
			throw new RedirectException();
		}
	}

	@Override
	public void redirect(String U) throws RedirectException {
		redirect(U, 301);
	}

	public void halt(int status, String reason) throws HaltException {
		// halt() only works for before()
		if (before) {
			throw new HaltException(status, reason);
		}
	}
}
