package dk.langli.esso;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;

public class GeneralEsso {
	private static String SWITCH_ALIAS_DSL = null;
	private JsonHttp http = null;
	private ObjectMapper mapper = new ObjectMapper();
	private boolean transactional = false;

	public GeneralEsso(String url, boolean transactional) throws EssoException {
		try {
			SWITCH_ALIAS_DSL = getJsonResource("switch-alias");
			http = new JsonHttp(url);
		}
		catch(MalformedURLException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"));
		mapper.setVisibilityChecker(new JacksonVisibilityChecker());
		SerializationConfig serializationConfig = mapper.getSerializationConfig();
		VisibilityChecker<JacksonVisibilityChecker> jacksonVisibilityChecker = new JacksonVisibilityChecker();
		jacksonVisibilityChecker = jacksonVisibilityChecker.with(JsonAutoDetect.Visibility.NONE);
		jacksonVisibilityChecker = jacksonVisibilityChecker.withFieldVisibility(JsonAutoDetect.Visibility.NONE);
		jacksonVisibilityChecker = jacksonVisibilityChecker.withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY);
		jacksonVisibilityChecker = jacksonVisibilityChecker.withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY);
		jacksonVisibilityChecker = jacksonVisibilityChecker.withCreatorVisibility(JsonAutoDetect.Visibility.NONE);
		serializationConfig.with(jacksonVisibilityChecker);
		setTransactional(transactional);
	}
	
	@SuppressWarnings("unchecked")
	public void setTransactional(boolean transactional) {
		this.transactional = transactional;
		if(transactional) {
			http.addParam("refresh", "true", HttpPut.class, HttpPost.class, HttpDelete.class);
		}
		else {
			http.removeParam("refresh", HttpPut.class, HttpPost.class, HttpDelete.class);
		}
	}
	
	public boolean isTransactional() {
		return transactional;
	}

	public void createIndex(String name) throws EssoException {
		createIndex(name, null, null, null);
	}

	public void createIndex(String name, Integer numberOfShards, Integer numberOfReplicas) throws EssoException {
		createIndex(name, numberOfShards, numberOfReplicas, null);
	}

	public void createIndex(String name, Integer numberOfShards, Integer numberOfReplicas, @SuppressWarnings("rawtypes") Map mappings) throws EssoException {
		Map<String, Object> dsl = map();
		if(numberOfShards != null || numberOfReplicas != null) {
			Map<String, Object> settings = map();
			dsl.put("settings", settings);
			if(numberOfShards != null) {
				settings.put("numberOfShards", numberOfShards);
			}
			if(numberOfReplicas != null) {
				settings.put("numberOfReplicas", numberOfReplicas);
			}
		}
		if(mappings != null) {
			dsl.put("mappings", mappings);
		}
		try {
			expect(http.put(name, toJson(dsl)), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(JsonProcessingException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}
	
	public void deleteIndex(String name) throws EssoException {
		try {
			expect(http.delete(name), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public void openIndex(String name) throws EssoException {
		try {
			expect(http.post(name+"/_open"), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public void closeIndex(String name) throws EssoException {
		try {
			expect(http.post(name+"/_close"), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}
	
	public Long saveDocument(String index, String type, Object document) throws EssoException {
		Long version = null;
		try {
			String json = toJson(document);
			String path = index+"/"+type;
			String _version = null;
			HttpInputStream response = null;
			if(document instanceof Idable && ((Idable) document).get_id() != null) {
				if(document instanceof Revisable && ((Revisable) document).get_version() != null) {
					_version = "version="+((Revisable) document).get_version();
				}
				path = path+"/"+((Idable) document).get_id();
				response = expect(http.put(param(path, _version), json), 200);
			}
			else {
				response = expect(http.post(path, json), 201);
			}
			JsonNode root = mapper.readTree(response);
			version = root.path("_version").asLong();
			response.close();
		}
		catch(JsonProcessingException e) {
			wrap(e);
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return version;
	}

	public Long saveDocument(String index, String type, String json) throws EssoException {
		Long version = null;
		try {
			HttpInputStream response = expect(http.put(index+"/"+type, json), 201);
			JsonNode root = mapper.readTree(response);
			version = root.path("_version").asLong();
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return version;
	}

	public Long saveDocument(String index, String type, String id, String json) throws EssoException {
		Long version = null;
		try {
			HttpInputStream response = expect(http.put(index+"/"+type+"/"+id, json), 200, 201);
			JsonNode root = mapper.readTree(response);
			version = root.path("_version").asLong();
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return version;
	}

	public <T> T getDocument(String index, String type, String id, Class<T> mappedType) throws EssoException {
		T document = null;
		try {
			String url = index+"/"+type+"/"+id;
			HttpInputStream response = expect(http.get(url), 200);
			document = fromJson(response, mappedType);
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return document;
	}

	public <T> List<T> getDocuments(String index, String type, Class<T> mappedType) throws EssoException {
		return getDocuments(index, type, mappedType, null);
	}

	public <T> List<T> getDocuments(String index, String type, Class<T> mappedType, @SuppressWarnings("rawtypes") Map queryDsl) throws EssoException {
		List<T> documents = new ArrayList<T>();
		try {
			String json = toJson(queryDsl);
			String url = index+"/"+type+"/_search";
			if(Revisable.class.isAssignableFrom(mappedType)) {
				url = param(url, "version");
			}
			HttpInputStream response = expect(http.get(url, json), 200);
			JsonNode root = mapper.readTree(response);
			Iterator<JsonNode> hits = root.path("hits").path("hits").iterator();
			while(hits.hasNext()) {
				JsonNode hit = hits.next();
				T document = fromJson(hit, mappedType);
				documents.add(document);
			}
			response.close();
		}
		catch(JsonProcessingException e) {
			wrap(e);
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return documents;
	}

	public void deleteDocument(String index, String type, Idable document) throws EssoException {
		try {
			expect(http.delete(index+"/"+type+"/"+document.get_id()), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public void deleteDocument(String index, String type, String id) throws EssoException {
		try {
			expect(http.delete(index+"/"+type+"/"+id), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public void addAlias(String index, String alias) throws EssoException {
		try {
			expect(http.put(index+"/_alias/"+alias), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public List<String> listIndices() throws EssoException {
		List<String> indices = new ArrayList<String>();
		try {
			HttpInputStream response = expect(http.get("_status"), 200);
			JsonNode root = mapper.readTree(response);
			Iterator<String> fieldNames = root.path("indices").fieldNames();
			while(fieldNames.hasNext()) {
				indices.add(fieldNames.next());
			}
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return indices;
	}

	public List<String> listIndices(String alias) throws EssoException {
		List<String> indices = new ArrayList<String>();
		try {
			HttpInputStream response = expect(http.get("*/_alias/"+alias), 200);
			JsonNode root = mapper.readTree(response);
			Iterator<String> fieldNames = root.fieldNames();
			while(fieldNames.hasNext()) {
				indices.add(fieldNames.next());
			}
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return indices;
	}
	
	public List<String> listAliases(String index) throws EssoException {
		List<String> aliases = new ArrayList<String>();
		try {
			HttpInputStream response = expect(http.get(index+"/_alias/*"), 200);
			JsonNode root = mapper.readTree(response);
			Iterator<String> fieldNames = root.path(index).path("aliases").fieldNames();
			while(fieldNames.hasNext()) {
				aliases.add(fieldNames.next());
			}
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return aliases;
	}

	public void switchAlias(String alias, String fromIndex, String toIndex) throws EssoException {
		Map<String, Object> variables = map();
		variables.put("alias", alias);
		variables.put("fromIndex", fromIndex);
		variables.put("toIndex", toIndex);
		String json = StrSubstitutor.replace(SWITCH_ALIAS_DSL, variables);
		try {
			expect(http.post("_aliases", json), 200).close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
	}

	public Long count(String index) throws EssoException {
		return countPath(index);
	}

	public Long count(String index, String type) throws EssoException {
		return countPath(index+"/"+type);
	}

	private Long countPath(String path) throws EssoException {
		Long count = null;
		try {
			HttpInputStream response = expect(http.get(path+"/_count"), 200);
			JsonNode root = mapper.readTree(response);
			count = root.path("count").asLong();
			response.close();
		}
		catch(ClientProtocolException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		catch(HttpResponseCodeException e) {
			wrap(e);
		}
		return count;
	}

	private void wrap(Throwable e) throws EssoException {
		wrap(e.getMessage(), e);
	}
	
	private void wrap(String msg, Throwable e) throws EssoException {
		throw new EssoException(msg, e);
	}
	
	private Map<String, Object> map() {
		return new HashMap<String, Object>();
	}
	
	private String param(String url, String parameter) {
		return parameter != null ? url+(url.indexOf('?') == -1 ? "?" : "&")+parameter : url;
	}

	private HttpInputStream expect(HttpInputStream response, int... expectedStatus) throws HttpResponseCodeException {
		boolean success = false;
		int status = response.status();
		for(int expected: expectedStatus) {
			if(status == expected) {
				success = true;
			}
		}
		if(!success) {
			throw new HttpResponseCodeException(response, expectedStatus);
		}
		return response;
	}
	
	private String toJson(Object document) throws JsonProcessingException {
		return document != null ? mapper.writeValueAsString(document) : null;
	}

	private <T> T fromJson(InputStream elasticSearchResponse, Class<T> mappedType) throws EssoException {
		T document = null;
		try {
			document = fromJson(mapper.readTree(elasticSearchResponse), mappedType);
		}
		catch(JsonProcessingException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		return document;
	}

	private <T> T fromJson(JsonNode root, Class<T> mappedType) throws EssoException {
		T document = null;
		try {
			JsonParser parser = root.path("_source").traverse();
			document = mapper.readValue(parser, mappedType);
			if(document instanceof Idable) {
				((Idable) document).set_id(root.path("_id").asText());
				if(document instanceof Revisable) {
					((Revisable) document).set_version(root.path("_version").asLong());
				}
			}
		}
		catch(JsonParseException e) {
			wrap(e);
		}
		catch(JsonMappingException e) {
			wrap(e);
		}
		catch(IOException e) {
			wrap(e);
		}
		return document;
	}
	
	private String getJsonResource(String name) throws IOException {
		return IOUtils.toString(GeneralEsso.class.getClassLoader().getResourceAsStream(name+".json"));
		
	}
}
