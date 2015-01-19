package dk.nine.esso;

public class RevisableDocument extends IdableDocument implements Revisable {
	@Override
	public Long get_version() {
		return super.get_version();
	}

	@Override
	public void set_version(Long _version) {
		super.set_version(_version);
	}
}
