package com.microservices.confluentKafka.adapter.atic.api.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.microservices.confluentKafka.adapter.atic.BaseClientAdapter;
import com.microservices.confluentKafka.adapter.atic.api.ClientAdapter;

@Service
public class ClientAdapterImpl extends BaseClientAdapter implements ClientAdapter {
	
	private final Logger logger = LoggerFactory.getLogger(ClientAdapterImpl.class);

	@Override
	public ResponseEntity<String> paramRequest(String body, String uri) throws Exception {
		HttpMethod method = HttpMethod.POST;
		MediaType mediaType = MediaType.APPLICATION_JSON;
		return sendRequest(uri, body, method);
	}
	
}
