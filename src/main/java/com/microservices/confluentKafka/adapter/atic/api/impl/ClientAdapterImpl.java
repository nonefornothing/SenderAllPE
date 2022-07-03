package com.microservices.confluentKafka.adapter.atic.api.impl;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.microservices.confluentKafka.adapter.atic.BaseClientAdapter;
import com.microservices.confluentKafka.adapter.atic.api.ClientAdapter;

@Service
public class ClientAdapterImpl extends BaseClientAdapter implements ClientAdapter {

	@Override
	public ResponseEntity<String> paramRequest(String body, String uri) throws Exception {
		HttpMethod method = HttpMethod.POST;
		return sendRequest(uri, body, method);
	}
	
}
