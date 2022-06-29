package com.microservices.confluentKafka.adapter.atic.api;

import org.springframework.http.ResponseEntity;

public interface ClientAdapter {

	ResponseEntity<String> paramRequest(String body, String uri) throws Exception;

}
