package dk.nine.esso;

@SuppressWarnings("serial")
public class EssoException extends Exception {

	public EssoException() {
		super();
	}

	public EssoException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public EssoException(String message, Throwable cause) {
		super(message, cause);
	}

	public EssoException(String message) {
		super(message);
	}

	public EssoException(Throwable cause) {
		super(cause);
	}
}
