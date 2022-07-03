package com.microservices.confluentKafka.adapter.atic;

import java.util.Base64;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class BaseClientAdapter {

	@Value("${adapter.client.timeout}")
	private int adapterClientTimeout;
	
	@Value("${pe.user}")
	private String peUser;
	
	@Value("${pe.password}")
	private String pePassword;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ResponseEntity<String> sendRequest(String url, String body, HttpMethod method) throws Exception {
		RestTemplate restTemplate = new RestTemplate(getClientHttpRequestFactory(adapterClientTimeout));
		HttpHeaders headers = setHeaders();
	    HttpEntity request = new HttpEntity(body,headers);
		return restTemplate.exchange(url, method, request, String.class);
	}
	
	// set timeout
	private SimpleClientHttpRequestFactory getClientHttpRequestFactory(int timeout){
	    SimpleClientHttpRequestFactory clientHttpRequestFactory= new SimpleClientHttpRequestFactory();
	    //Connect timeout
	    clientHttpRequestFactory.setConnectTimeout(timeout);
	    //Read timeout
	    clientHttpRequestFactory.setReadTimeout(timeout);
	    return clientHttpRequestFactory;
	}
	
	//set header for authentication
	private HttpHeaders setHeaders() {
		String authStr = peUser+":"+pePassword;
	    String base64Creds = Base64.getEncoder().encodeToString(authStr.getBytes());
	    HttpHeaders headers = new HttpHeaders();
	    headers.add("Authorization", "Basic " + base64Creds);
	    headers.add("Accept", "application/json");
	    headers.add("Content-Type", "application/json");
		return headers;
	}	
	
}
