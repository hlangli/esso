package dk.langli.esso;

import java.util.List;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import dk.langli.esso.Esso;
import dk.langli.esso.EssoException;

public class EssoTest {
	private static final Node node = new NodeBuilder().local(true).node();
	private static final String INDEX = "esso-test";
	private static Esso<RevisableDocument> esso = null;

	public EssoTest() throws EssoException {
		esso = new Esso<RevisableDocument>("http://localhost:9200/", RevisableDocument.class);
	}
	
	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testRevisable() throws EssoException {
		esso.createIndex(INDEX);
		RevisableDocument document = new RevisableDocument();
		document.setId("1");
		document.setVersion(1L);
		document.setMessage("Øllebrød!");
		esso.saveDocument(INDEX, document);
		List<RevisableDocument> documents = esso.getDocuments(INDEX);
		assert documents.size() == 1;
		RevisableDocument essoDoc = documents.get(0);
		assert essoDoc.getId().equals("1");
		assert essoDoc.getVersion() == 1L;
		assert essoDoc.getMessage().equals("Øllebrød!");
		assert essoDoc.get_id() != null;
		assert essoDoc.get_version() == 1L;
		Long version = esso.saveDocument(INDEX, essoDoc);
		assert version == 2L;
		essoDoc = esso.getDocument(INDEX, essoDoc.get_id());
		assert essoDoc.get_version() == 2L;
		assert esso.count(INDEX) == 1L;
		esso.deleteDocument(INDEX, essoDoc);
		assert esso.getDocuments(INDEX).size() == 0;
		assert esso.count(INDEX) == 0L;
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
