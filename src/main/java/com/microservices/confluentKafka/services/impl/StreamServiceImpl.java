package com.microservices.confluentKafka.services.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.microservices.confluentKafka.adapter.atic.api.ClientAdapter;
import com.microservices.confluentKafka.services.StreamService;

@Service
public class StreamServiceImpl implements StreamService {
	
	@Autowired
	private ClientAdapter clientAdapter;
	
	@Override
	public ResponseEntity<String> sendData(String request, String uri) throws Exception {
		return clientAdapter.paramRequest( request,uri);
	}

}
