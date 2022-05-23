package com.microservices.confluentKafka.adapter.atic.api.impl;

import java.net.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import com.microservices.confluentKafka.adapter.atic.BaseClientAdapter;
import com.microservices.confluentKafka.adapter.atic.api.ClientAdapter;


/**
 * This services for anabatic api
 * 
 * @author bwx
 * @date 12-02-2020
 * 
 */


@Service
public class ClientAdapterImpl extends BaseClientAdapter implements ClientAdapter {
	
	private final Logger logger = LoggerFactory.getLogger(ClientAdapterImpl.class);
	
	@Value("${base.url.atic.api}")
	private String baseUrlApiAtic;
	
	@Value("${api.atic.healt.check}")
	private String urlHealtCheck;
	
	 /**
    *
    * @author BWX
    * @Date 2020-03-12
    * CLient adapter for REST client
    * 
    */
	
	@Override
	public String paramRequest(String body,String uri) throws ConnectException  {
		HttpMethod method = HttpMethod.POST;
		MediaType mediaType = MediaType.APPLICATION_JSON;
//		System.out.println("url : " + uri);
		return sendRequest(uri, body, method, mediaType);
	
	}

	@Override
	public String healtCheck() {
		String result = null;
		final String uri = baseUrlApiAtic + urlHealtCheck;
//		System.out.println("url : " + uri);
		HttpMethod method = HttpMethod.GET;
		 MediaType mediaType = MediaType.APPLICATION_JSON;
		try {
			result = healtCheck(uri, method, mediaType);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error healt check mansek ...!! " + e.getMessage());
		}
		return result;
	}
	
}
