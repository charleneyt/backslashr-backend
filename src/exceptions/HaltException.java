package exceptions;

public class HaltException extends Exception {
	public int statusCode;
	public String reasonPhrase;

	public HaltException(int status, String reason) {
		this.statusCode = status;
		this.reasonPhrase = reason;

	}
}