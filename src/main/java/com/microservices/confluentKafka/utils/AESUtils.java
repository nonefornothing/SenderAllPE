package com.microservices.confluentKafka.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.security.Key;

 
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
 
public class AESUtils {

	private static SecretKeySpec secretKey;
	private static byte[] key;

	/**
	 * AES configuration for decode and encode message with secret key
	 * 
	 */

	public static void setKey(String myKey) {
		MessageDigest sha = null;
		try {
			key = myKey.getBytes("UTF-8");
			sha = MessageDigest.getInstance("SHA-1");
			key = sha.digest(key);
			key = Arrays.copyOf(key, 16);
			secretKey = new SecretKeySpec(key, "AES");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public static String encrypt(String strToEncrypt, String secret) {
		try {
			String result = encrypts(strToEncrypt, secret);
			try {
			    decrypt(result, secret);
			} catch (Exception e) {
				result = encrypts(strToEncrypt, secret);
			}
			return result;
		} catch (Exception e) {
			try {
				String result = encrypts(strToEncrypt, secret);
				try {
				    decrypt(result, secret);
				} catch (Exception e1) {
					result = encrypts(strToEncrypt, secret);
				}
				return result;
			} catch (Exception e1) {
				System.out.println("Error while encrypting: " + e1.toString());
				
			}
			
		}
		return null;
	}
	
	public static String encrypts(String strToEncrypt, String secret) {
		try {
			setKey(secret);
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			String result = Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
			return result;
		} catch (Exception e) {
			System.out.println("Error while encrypting: " + e.toString());
		}
		return null;
	}

	public static String decrypt(String strToDecrypt, String secret) {
		try {
			setKey(secret);
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
			return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error while decrypting: " + e.toString());
		}
		return null;
	}

 

//	  public static void main(String[] args) { 
//			final String secretKey = "kafkaadmin";
//	  
//	  String originalString =
//	  "ya/eO+85Rxh2CytePWRKDf/p0lCe+wRLQoGPDLG8j1dpYWfneCdaHzwJSLO9EwPvIJV6YagnVq8DZPb0MJuajcypCsbCkXfIIoOFHZ4LKv4quVHg7WaRpkxyjXNkA01YdwtUi26wlN4DIi/Ys2ak7hdENSZCZ3dllpCwyd+L/4BdbUk3dxsxkylUCVoTo9kuqDDjTbvUPVG3uRLy2Tulbdlp1mwGQlr7tE9qTRXt9qicWQhyhb/zcjun/v+hTOfKGF+cGJg0TkIdLj/ze/T7oRhfnBiYNE5CHS4/83v0+6Gl6pBA/DV4WAOwnjh1slPpSYlFpokzBt1Fgk5B4nlQfLXWTsVIBMr6N9ga92JDeEwPQPYxsj6X5S+tggXy//fGYM2c46DCMve5MUrV6KrKyEaOpBWnW34p1jEhDuzJi6pKdkGaR/leBb7OO5FqlISS3suVY/FmRvRPHZnLZ+iSuA8hiMSmnAROz+0c7Z4yEskYX5wYmDROQh0uP/N79PuhGF+cGJg0TkIdLj/ze/T7oRhfnBiYNE5CHS4/83v0+6EYX5wYmDROQh0uP/N79PuhGF+cGJg0TkIdLj/ze/T7oRhfnBiYNE5CHS4/83v0+6H6OTWZJw4ZjGR+9PueGb25fW2qiGYmHEnoRoYHhdvEkkIj8P7GGqrxYVt7+STNoUdPyXJdnE09DtDjBVGgTfC7+ow8HlEsgDMTkXICyrw8lQ==";
//	  // String encryptedString = AESUtils.encrypt(originalString, secretKey) ;
//	  String decryptedString = AESUtils.decrypt(originalString, secretKey) ;
//	  
//	  System.out.println(originalString); // System.out.println(encryptedString);
//	  System.out.println(decryptedString); }
	
    
    
}