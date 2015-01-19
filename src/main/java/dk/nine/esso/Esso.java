package dk.nine.esso;

import java.util.List;
import java.util.Map;

public class Esso<T> {
	private Class<T> mappedType = null;
	private GeneralEsso esso = null;
	private String type = null;
	
	public Esso(String url, Class<T> mappedType) throws EssoException {
		this.mappedType = mappedType;
		type = mappedType.getSimpleName();
		esso = new GeneralEsso(url, true);
	}
	
	public void setTransactional(boolean transactional) {
		esso.setTransactional(transactional);
	}

	public boolean isTransactional() {
		return esso.isTransactional();
	}

	public void createIndex(String name) throws EssoException {
		esso.createIndex(name);
	}

	public void createIndex(String name, Integer numberOfShards, Integer numberOfReplicas) throws EssoException {
		esso.createIndex(name, numberOfShards, numberOfReplicas);
	}

	public void createIndex(String name, Integer numberOfShards, Integer numberOfReplicas, @SuppressWarnings("rawtypes") Map mappings) throws EssoException {
		esso.createIndex(name, numberOfShards, numberOfReplicas, mappings);
	}

	public void deleteIndex(String name) throws EssoException {
		esso.deleteIndex(name);
	}

	public void openIndex(String name) throws EssoException {
		esso.openIndex(name);
	}

	public void closeIndex(String name) throws EssoException {
		esso.closeIndex(name);
	}

	public Long saveDocument(String index, T document) throws EssoException {
		return esso.saveDocument(index, type, document);
	}

	public T getDocument(String index, String id) throws EssoException {
		return esso.getDocument(index, type, id, mappedType);
	}

	public List<T> getDocuments(String index) throws EssoException {
		return esso.getDocuments(index, type, mappedType);
	}

	public List<T> getDocuments(String index, @SuppressWarnings("rawtypes") Map queryDsl) throws EssoException {
		return esso.getDocuments(index, type, mappedType, queryDsl);
	}

	public void deleteDocument(String index, Idable document) throws EssoException {
		esso.deleteDocument(index, type, document);
	}

	public void deleteDocument(String index, String id) throws EssoException {
		esso.deleteDocument(index, type, id);
	}

	public void addAlias(String index, String alias) throws EssoException {
		esso.addAlias(index, alias);
	}

	public List<String> listIndices() throws EssoException {
		return esso.listIndices();
	}

	public List<String> listIndices(String alias) throws EssoException {
		return esso.listIndices(alias);
	}

	public List<String> listAliases(String index) throws EssoException {
		return esso.listAliases(index);
	}

	public void switchAlias(String alias, String fromIndex, String toIndex) throws EssoException {
		esso.switchAlias(alias, fromIndex, toIndex);
	}

	public Long count(String index) throws EssoException {
		return esso.count(index, type);
	}
}
