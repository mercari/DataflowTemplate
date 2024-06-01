package com.mercari.solution.util;

import com.google.gson.Gson;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CryptoUtil {

    private static final Gson gson = new Gson();

    private static final String RECV_WINDOW = "5000";


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

    private static String findAlgorithmForKeySpec(final String algorithm) {
        return switch (algorithm.toUpperCase()) {
            case "AES", "AES256" -> "AES";
            default -> throw new IllegalArgumentException("Not found algorithm: " + algorithm);
        };
    }

    private static String findAlgorithmForCipher(final String algorithm) {
        return switch (algorithm.toUpperCase()) {
            case "AES", "AES256" -> "AES/CTR/PKCS5Padding";
            default -> throw new IllegalArgumentException("Not found algorithm: " + algorithm);
        };
    }


    public static String generatePostSignature(final Mac mac, final String key, final long epochMillis, Map<String, ?> params) {
        final String paramJson = gson.toJson(params);
        final String sb = epochMillis + key + RECV_WINDOW + paramJson;
        return bytesToHex(mac.doFinal(sb.getBytes(StandardCharsets.UTF_8)));
    }

    public static String generateGetSignature(final Mac mac, final String key, final long epochMillis, Map<String, ?> params) {
        final StringBuilder sb = createQueryParameterStr(params);
        final String queryStr = epochMillis + key + RECV_WINDOW + sb;
        return bytesToHex(mac.doFinal(queryStr.getBytes(StandardCharsets.UTF_8)));
    }

    private static String bytesToHex(byte[] hash) {
        final StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            final String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static StringBuilder createQueryParameterStr(Map<String, ?> map) {
        final Iterator<String> itr = map.keySet().iterator();
        final StringBuilder sb = new StringBuilder();
        while (itr.hasNext()) {
            final String key = itr.next();
            sb.append(key)
                    .append("=")
                    .append(map.get(key))
                    .append("&");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb;
    }
}
