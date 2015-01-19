package dk.nine.esso;

import java.io.IOException;

import org.apache.commons.io.IOUtils;

@SuppressWarnings("serial")
public class HttpResponseCodeException extends Exception {
	public HttpResponseCodeException(HttpInputStream response, int[] expectedResponseCodes) {
		super(createErrorMessage(response, expectedResponseCodes));
	}
	
	private static String createErrorMessage(HttpInputStream response, int[] expectedResponseCodes) {
		String str = null;
		try {
			str = IOUtils.toString(response);
		}
		catch(IOException e) {
			str = "ElasticSearch responded "+response.status()+", but expected response code was in "+toString(expectedResponseCodes);
		}
		return str;
	}
	
	private static String toString(int[] integers) {
		String str = "[";
		for(int i=0; i<integers.length; i++) {
			str += (i>0 ? ", " : "")+integers[i];
		}
		str += "]";
		return str;
	}
}
