package ex_8_bonus;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class DecryptModule {
    private static final String ALGORITHM = "AES";
    private static final String KEY = "ZeroXTZKey123456";

    public static String decrypt(String encryptedString) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
            byte[] decodedBytes = Base64.getDecoder().decode(encryptedString);
            byte[] decryptedBytes = cipher.doFinal(decodedBytes);
            return new String(decryptedBytes);
        } catch (Exception e){
            System.err.println("Error decrypting string: "+ e.getMessage());
            return null;
        }
    }

    // public static void main(String[] args) throws Exception {
    //     String encryptedString = "nR2zP3q8H+DFOWkc17sCLw==";
    //     String decryptedString = decrypt(encryptedString);
    //     System.out.println("Encrypted string: "+ encryptedString);
    //     System.out.println("Decrypted string: "+ decryptedString);
    // }
}