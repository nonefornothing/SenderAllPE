package com.microservices.confluentKafka.adapter.atic.api;

import java.net.ConnectException;

public interface ClientAdapter {

	String paramRequest(String body, String uri) throws ConnectException;
	
	String healtCheck();

}
