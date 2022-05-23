package com.microservices.confluentKafka.utils;

import javax.net.ssl.HttpsURLConnection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SSLUtils {
	
	
	@Value("${ssl.keyStore.file}")
	private String sslKeyStoreFile;
	
	@Value("${ssl.keyStore.pass}")
	private String sslKeyStorePass;
	
	@Value("${ssl.trustStore.file}")
	private String sslTrustStoreFile;
	
	@Value("${ssl.trustStore.pass}")
	private String sslTrustStorePass;
	
	@Value("${ssl.host}")
	private String sslHost;
	
	
	/**
	 * Setting configuration for SSL certificate
	 * 
	 */
	
	@Bean
	public void setKeySSL(){
		HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> hostname.equals(sslHost));
		System.setProperty("javax.net.ssl.trustStore", sslTrustStoreFile);
		System.setProperty("javax.net.ssl.trustStorePassword", sslTrustStorePass);
		System.setProperty("javax.net.ssl.keyStore", sslKeyStoreFile);
		System.setProperty("javax.net.ssl.keyStorePassword", sslKeyStorePass);
	}

}
