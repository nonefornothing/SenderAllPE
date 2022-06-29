package com.microservices.confluentKafka.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;


 
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
 
public class AESUtils {

	private static SecretKeySpec secretKey;

	/**
	 * AES configuration for decode and encode message with secret key
	 * 
	 */

	public static void setKey(String myKey) {
		MessageDigest sha;
		try {
			byte[] key = myKey.getBytes("UTF-8");
			sha = MessageDigest.getInstance("SHA-1");
			key = sha.digest(key);
			key = Arrays.copyOf(key, 16);
			secretKey = new SecretKeySpec(key, "AES");
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
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
				System.out.println("Error while encrypting: " + e1);
				
			}
			
		}
		return null;
	}
	
	public static String encrypts(String strToEncrypt, String secret) {
		try {
			setKey(secret);
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
		} catch (Exception e) {
			System.out.println("Error while encrypting: " + e);
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
			System.out.println("Error while decrypting: " + e);
		}
		return null;
	}
    
}