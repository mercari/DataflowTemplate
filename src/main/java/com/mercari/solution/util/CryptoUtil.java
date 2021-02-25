package com.mercari.solution.util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;
import java.util.Base64;

public class CryptoUtil {

    public byte[] decrypt(
            final String algorithm,
            final byte[] keyBytes,
            final byte[] encryptedBytes) throws Exception {

        if(encryptedBytes == null || encryptedBytes.length == 0) {
            return null;
        }

        final java.security.Key key = new SecretKeySpec(keyBytes, findAlgorithmForKeySpec(algorithm));
        final String cipherAlgorithm = findAlgorithmForCipher(algorithm);
        final Cipher decrypter = Cipher.getInstance(cipherAlgorithm);
        final byte[] dataToDecrypt;
        // Block cipher algorithm such as AES/CTR must set block size.
        if("AES/CTR/PKCS5Padding".equals(cipherAlgorithm)) {
            final IvParameterSpec iv = new IvParameterSpec(Arrays.copyOf(encryptedBytes, decrypter.getBlockSize()));
            decrypter.init(Cipher.DECRYPT_MODE, key, iv);
            dataToDecrypt = Arrays.copyOfRange(encryptedBytes, decrypter.getBlockSize(), encryptedBytes.length);
        } else {
            throw new IllegalArgumentException(cipherAlgorithm + " is not supported!");
        }
        return decrypter.doFinal(dataToDecrypt);
    }

    public String toString(byte[] bytes) {
        return new String(bytes);
    }

    public String encodeBase64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public byte[] decodeBase64(String text) {
        return Base64.getDecoder().decode(text);
    }

    private static String findAlgorithmForKeySpec(final String algorithm) {
        switch (algorithm.toUpperCase()) {
            case "AES":
            case "AES256":
                return "AES";
            default:
                throw new IllegalArgumentException("Not found algorithm: " + algorithm);
        }
    }

    private static String findAlgorithmForCipher(final String algorithm) {
        switch (algorithm.toUpperCase()) {
            case "AES":
            case "AES256":
                return "AES/CTR/PKCS5Padding";
            default:
                throw new IllegalArgumentException("Not found algorithm: " + algorithm);
        }
    }

}
