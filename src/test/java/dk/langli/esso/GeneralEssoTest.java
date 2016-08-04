package dk.langli.esso;

import java.util.List;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import dk.langli.esso.EssoException;
import dk.langli.esso.GeneralEsso;

public class GeneralEssoTest {
	private static final Node node = new NodeBuilder().local(true).node();
	private static final String INDEX = "esso-test";
	private static final String DOCUMENT_TYPE = "esso-doc";
	private static final String ALIAS = "esso-alias";
	private static final String MESSAGE = "Je suis Charlie!";
	private static GeneralEsso esso = null;

	public GeneralEssoTest() throws EssoException {
		esso = new GeneralEsso("http://localhost:9200/", true);
	}
	
	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testIndex() throws EssoException {
		System.out.println("Running testIndex()");
		esso.createIndex(INDEX);
		List<String> indices = esso.listIndices();
		assert indices.size() == 1;
		assert indices.get(0).equals(INDEX);
		esso.deleteIndex(INDEX);
		assert esso.listIndices().size() == 0;
	}

	@Test
	public void testAlias() throws EssoException {
		System.out.println("Running testAlias()");
		String index1 = INDEX+"1";
		String index2 = INDEX+"2";
		esso.createIndex(index1);
		esso.addAlias(index1, ALIAS);
		List<String> aliases = esso.listAliases(index1);
		assert aliases.size() == 1;
		assert aliases.get(0).equals(ALIAS);
		esso.createIndex(index2);
		esso.switchAlias(ALIAS, index1, index2);
		aliases = esso.listAliases(index2);
		assert aliases.size() == 1;
		assert aliases.get(0).equals(ALIAS);
		assert esso.listAliases(index1).size() == 0;
	}

	private <T extends Document> T generalDocumentTest(Class<T> type) throws EssoException {
		esso.createIndex(INDEX);
		T document = null;
		try {
			document = type.newInstance();
		}
		catch(InstantiationException e) {
			e.printStackTrace();
		}
		catch(IllegalAccessException e) {
			e.printStackTrace();
		}
		document.setId("1");
		document.setVersion(1L);
		document.setMessage(MESSAGE);
		esso.saveDocument(INDEX, DOCUMENT_TYPE, document);
		List<T> documents = esso.getDocuments(INDEX, DOCUMENT_TYPE, type);
		assert documents.size() == 1;
		T essoDoc = documents.get(0);
		assert essoDoc.getId().equals("1");
		assert essoDoc.getVersion() == 1L;
		assert essoDoc.getMessage().equals(MESSAGE);
		return essoDoc;
	}

	@Test
	public void testDocument() throws EssoException {
		System.out.println("Running testDocument()");
		Document essoDoc = generalDocumentTest(Document.class);
		assert essoDoc.get_id() == null;
		assert essoDoc.get_version() == null;
	}

	@Test
	public void testIdable() throws EssoException {
		System.out.println("Running testIdable()");
		IdableDocument essoDoc = generalDocumentTest(IdableDocument.class);
		assert essoDoc.get_id() != null;
		assert essoDoc.get_version() == null;
		essoDoc = esso.getDocument(INDEX, DOCUMENT_TYPE, essoDoc.get_id(), IdableDocument.class);
		assert essoDoc != null;
	}

	@Test
	public void testRevisable() throws EssoException {
		System.out.println("Running testRevisable()");
		RevisableDocument essoDoc = generalDocumentTest(RevisableDocument.class);
		assert essoDoc.get_id() != null;
		assert essoDoc.get_version() == 1L;
		Long version = esso.saveDocument(INDEX, DOCUMENT_TYPE, essoDoc);
		assert version == 2L;
		essoDoc = esso.getDocument(INDEX, DOCUMENT_TYPE, essoDoc.get_id(), RevisableDocument.class);
		assert essoDoc.get_version() == 2L;
		assert esso.count(INDEX) == 1L;
		assert esso.count(INDEX, DOCUMENT_TYPE) == 1L;
		esso.deleteDocument(INDEX, DOCUMENT_TYPE, essoDoc);
		assert esso.getDocuments(INDEX, DOCUMENT_TYPE, RevisableDocument.class).size() == 0;
		assert esso.count(INDEX) == 0L;
		assert esso.count(INDEX, DOCUMENT_TYPE) == 0L;
	}

	@After
	public void tearDown() throws Exception {
		List<String> indices = esso.listIndices();
		for(String index: indices) {
			esso.deleteIndex(index);
		}
	}

	@AfterClass
	public static void tearDownClass() {
		node.close();
	}
}
