package com.microservices.confluentKafka.adapter.atic;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class BaseClientAdapter {
	
	private final Logger logger = LoggerFactory.getLogger(BaseClientAdapter.class);
	
	@Value("${adapter.client.timeout}")
	private int adapterClientTimeout;
	
	@Value("${healtCheck.client.timeout}")
	private int healtCheckClientTimeout;
	
	@Value("${mansek.user}")
	private String mansekUser;
	
	@Value("${mansek.password}")
	private String mansekPassword;
	
	 /**
    *
    * @author BWX
    * @Date 2020-03-12
    * Base client adapter to client
    * 
    */
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String sendRequest(String url, String body, HttpMethod method, MediaType mediaType) throws ConnectException {
		RestTemplate restTemplate = new RestTemplate(getClientHttpRequestFactory(adapterClientTimeout));
		HttpHeaders headers = setHeaders();
//		headers.setAccept(Arrays.asList(mediaType));
	    HttpEntity request = new HttpEntity(body,headers);
	    ResponseEntity<String> response = restTemplate.exchange(url, method, request, String.class);
	    return response.getBody();
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String healtCheck(String url, HttpMethod method, MediaType mediaType) {
		ResponseEntity<String> response = null;
		try {
			RestTemplate restTemplate = new RestTemplate(getClientHttpRequestFactory(healtCheckClientTimeout));
		    HttpEntity request = new HttpEntity(setHeaders());
		    response = restTemplate.exchange(url, method, request, String.class);
		} catch (Exception e) {
			logger.error("Destination API (MANSEK) is Down....!!");
			e.printStackTrace();
		}
		return response.getBody();
	}
	
	// set timeout
	private SimpleClientHttpRequestFactory getClientHttpRequestFactory(int timesOut){
	    SimpleClientHttpRequestFactory clientHttpRequestFactory= new SimpleClientHttpRequestFactory();
	    //Connect timeout
	    clientHttpRequestFactory.setConnectTimeout(timesOut);
	    //Read timeout
	    clientHttpRequestFactory.setReadTimeout(timesOut);
	    return clientHttpRequestFactory;
	}
	
	//set header for authentication
	private HttpHeaders setHeaders() {
		String authStr = mansekUser+":"+mansekPassword;
	    String base64Creds = Base64.getEncoder().encodeToString(authStr.getBytes());
	    HttpHeaders headers = new HttpHeaders();
	    headers.add("Authorization", "Basic " + base64Creds);
	    headers.add("Accept", "application/json");
	    headers.add("Content-Type", "application/json");
		return headers;
	}	
	
}
