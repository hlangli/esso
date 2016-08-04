package dk.langli.esso;

public interface Revisable extends Idable {
	public Long get_version();
	public void set_version(Long _version);
}
