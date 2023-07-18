package ex_8;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class EncryptModule {
	private static final String ALGORITHM = "AES";
	private static final String KEY = "ZeroXTZKey123456";
	
	public static String encrypt(String input) {
		try { 
			SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(),ALGORITHM);
			Cipher cipher = Cipher.getInstance(ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, keySpec);
			byte[] encryptedBytes = cipher.doFinal(input.getBytes());
			return java.util.Base64.getEncoder().encodeToString(encryptedBytes);
		} catch (Exception e){
			System.err.println("Error encrypting string: "+ e.getMessage());
			return null;
		}
	}
    // I tested it in Eclipse
	// public static void main(String[] args) throws Exception {
	// 	String originalString = "This is a secret message";
	// 	String encryptedString = encrypt(originalString);
	// 	System.out.println("Original string: "+ originalString);
	// 	System.out.println("Encrypted string: "+ encryptedString);
	// }
}