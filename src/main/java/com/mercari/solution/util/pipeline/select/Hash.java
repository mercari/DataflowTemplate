package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.gcp.SecretManagerUtil;
import org.apache.beam.sdk.schemas.Schema;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Hash implements SelectFunction {

    private static final String ALGORITHM_SHA256 = "SHA256";
    private static final String ALGORITHM_HMAC_SHA256 = "HmacSHA256";

    private final String name;
    private final List<String> fields;
    private final String algorithm;
    private final String secret;
    private final Integer size;
    private final String delimiter;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient MessageDigest messageDigest;
    private transient Mac mac;

    Hash(String name, List<String> fields, String secret, String algorithm, Integer size, String delimiter, boolean ignore) {
        this.name = name;
        this.fields = fields;
        this.secret = secret;
        this.algorithm = algorithm;
        this.size = size;
        this.delimiter = delimiter;

        this.inputFields = new ArrayList<>();
        for(final String field : fields) {
            this.inputFields.add(Schema.Field.of(field, Schema.FieldType.STRING.withNullable(true)));
        }
        this.outputFieldType = Schema.FieldType.STRING.withNullable(true);
        this.ignore = ignore;
    }

    public static Hash of(String name, JsonObject jsonObject, boolean ignore) {
        if(!jsonObject.has("field") && !jsonObject.has("fields")) {
            throw new IllegalArgumentException("SelectField hash: " + name + " requires field or fields parameter");
        }
        final List<String> fields = new ArrayList<>();
        if(jsonObject.has("field")) {
            final String field = jsonObject.get("field").getAsString();
            fields.add(field);
        } else if(jsonObject.has("fields")) {
            if(!jsonObject.get("fields").isJsonArray()) {
                throw new IllegalArgumentException("SelectField hash: " + name + " fields parameter must be array");
            }
            for(JsonElement element : jsonObject.getAsJsonArray("fields")) {
                fields.add(element.getAsString());
            }
        }

        final String algorithm;
        if(jsonObject.has("algorithm")) {
            algorithm = jsonObject.get("algorithm").getAsString();
        } else {
            algorithm = ALGORITHM_SHA256;
        }

        final String secret;
        if(jsonObject.has("secret")) {
            secret = jsonObject.get("secret").getAsString();
        } else {
            switch (algorithm) {
                case ALGORITHM_HMAC_SHA256 -> throw new IllegalArgumentException("SelectField hash: " + name + " requires parameter secret if algorithm is " + algorithm);
                default ->
                    secret = null;
            }
        }

        final Integer size;
        if(jsonObject.has("size")) {
            size = jsonObject.get("size").getAsInt();
        } else {
            size = null;
        }

        final String delimiter;
        if(jsonObject.has("delimiter")) {
            delimiter = jsonObject.get("delimiter").getAsString();
        } else {
            delimiter = "";
        }

        return new Hash(name, fields, secret, algorithm, size, delimiter, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        try {
            switch (algorithm) {
                case ALGORITHM_SHA256 -> {
                    this.messageDigest = MessageDigest.getInstance(this.algorithm);
                }
                case ALGORITHM_HMAC_SHA256 -> {
                    this.mac = Mac.getInstance(algorithm);
                    final String secret;
                    if(SecretManagerUtil.isSecretName(this.secret)) {
                        secret = SecretManagerUtil.getSecret(this.secret).toStringUtf8();
                    } else {
                        secret = this.secret;
                    }
                    final SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), algorithm);
                    this.mac.init(secretKeySpec);
                }
                default -> throw new IllegalArgumentException("hash algorithm: " + algorithm + " is not supported");
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Not supported algorithm error: ", e);
        } catch (final InvalidKeyException e) {
            throw new IllegalArgumentException("Invalid key error for secret: " + this.secret, e);
        }
    }

    @Override
    public Object apply(Map<String, Object> input) {
        final List<String> list = new ArrayList<>();
        for(final String field : fields) {
            Object value = input.get(field);
            if(value == null) {
                value = "";
            }
            list.add(value.toString());
        }

        final String str = String.join(delimiter, list);
        return hash(str.getBytes(StandardCharsets.UTF_8));
    }

    private String hash(final byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        final String output = switch (algorithm) {
            case ALGORITHM_SHA256 -> hashSHA256(bytes);
            case ALGORITHM_HMAC_SHA256 -> hashHMACSHA256(bytes);
            default -> throw new IllegalArgumentException("Not supported algorithm: " + algorithm);
        };

        if(size == null) {
            return output;
        } else if(size > output.length()) {
            return output;
        } else {
            return output.substring(0, size);
        }
    }

    private String hashSHA256(final byte[] bytes) {
        final byte[] hashed = messageDigest.digest(bytes);
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for(byte b : hashed) {
            sb.append(String.format("%02x", b&0xff));
        }
        return sb.toString();
    }

    private String hashHMACSHA256(final byte[] bytes) {
        final byte[] output = mac.doFinal(bytes);
        StringBuilder sb = new StringBuilder(output.length * 2);
        for(byte b : output) {
            sb.append(String.format("%02x", b&0xff));
        }
        return sb.toString();
    }

}