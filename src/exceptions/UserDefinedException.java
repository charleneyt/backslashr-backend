package exceptions;

public class UserDefinedException extends Exception {
	public String reasonPhrase;

	public UserDefinedException(String reason) {
		this.reasonPhrase = reason;
	}
}
