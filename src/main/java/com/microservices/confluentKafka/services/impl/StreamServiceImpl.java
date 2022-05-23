package com.microservices.confluentKafka.services.impl;

import java.net.ConnectException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.microservices.confluentKafka.adapter.atic.api.ClientAdapter;
import com.microservices.confluentKafka.services.StreamService;

@Service
public class StreamServiceImpl implements StreamService {
	
	@Autowired
	private ClientAdapter clientAdapter;
	
	@Value("${kafka.consumer.broker}")
	private String bootstrapServers;
	
	@Value("${kafka.consumer.id.group}")
	private String idGroupConsumer;
	
	@Override
	public String sendData(String request,String uri) throws ConnectException {
		return clientAdapter.paramRequest( request,uri);
	}
	
	@Override
	public String healtCheck() {
		return clientAdapter.healtCheck();
	}
}
