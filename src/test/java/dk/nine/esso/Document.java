package dk.nine.esso;

public class Document {
	private String id = null;
	private String _id = null;
	private Long _version = null;
	private Long version = null;
	private String message = null;
	
	public Document() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public Long get_version() {
		return _version;
	}

	public void set_version(Long _version) {
		this._version = _version;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
