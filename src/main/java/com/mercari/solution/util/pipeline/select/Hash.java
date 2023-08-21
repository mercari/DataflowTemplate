package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.util.gcp.SecretManagerUtil;
import org.apache.beam.sdk.schemas.Schema;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Hash implements SelectFunction {

    private static final String ALGORITHM_HMAC_SHA256 = "HmacSHA256";

    private final String name;
    private final String field;
    private final String algorithm;
    private final String secret;
    private final Integer size;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient Mac mac;

    Hash(String name, String field, String secret, String algorithm, Integer size, boolean ignore) {
        this.name = name;
        this.field = field;
        this.secret = secret;
        this.algorithm = algorithm;
        this.size = size;

        this.inputFields = new ArrayList<>();
        this.inputFields.add(Schema.Field.of(field, Schema.FieldType.STRING.withNullable(true)));
        this.outputFieldType = Schema.FieldType.STRING.withNullable(true);
        this.ignore = ignore;
    }

    public static Hash of(String name, JsonObject jsonObject, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField hash: " + name + " requires field parameter");
        }
        final String field = jsonObject.get("field").getAsString();

        final String algorithm;
        if(jsonObject.has("algorithm")) {
            algorithm = jsonObject.get("algorithm").getAsString();
        } else {
            algorithm = ALGORITHM_HMAC_SHA256;
        }

        final String secret;
        if(jsonObject.has("secret")) {
            secret = jsonObject.get("secret").getAsString();
        } else {
            switch (algorithm) {
                case ALGORITHM_HMAC_SHA256: {
                    throw new IllegalArgumentException("SelectField hash: " + name + " requires parameter secret if algorithm is " + algorithm);
                }
                default:
                    secret = null;
            }
        }

        final Integer size;
        if(jsonObject.has("size")) {
            size = jsonObject.get("size").getAsInt();
        } else {
            size = null;
        }

        return new Hash(name, field, secret, algorithm, size, ignore);
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
                case ALGORITHM_HMAC_SHA256: {
                    this.mac = Mac.getInstance(algorithm);
                    final String secret;
                    if(SecretManagerUtil.isSecretName(this.secret)) {
                        secret = SecretManagerUtil.getSecret(this.secret).toStringUtf8();
                    } else {
                        secret = this.secret;
                    }
                    final SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), algorithm);
                    this.mac.init(secretKeySpec);
                    break;
                }
                default:
                    throw new IllegalArgumentException("hash algorithm: " + algorithm + " is not supported");
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Not supported algorithm error: ", e);
        } catch (final InvalidKeyException e) {
            throw new IllegalArgumentException("Invalid key error for secret: " + this.secret, e);
        }
    }

    @Override
    public Object apply(Map<String, Object> input) {
        final Object value = input.get(field);
        if(value == null) {
            return null;
        }
        return hash(value.toString().getBytes(StandardCharsets.UTF_8));
    }

    private String hash(final byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        final String output;
        switch (algorithm) {
            case ALGORITHM_HMAC_SHA256:
                output = hashHMACSHA256(bytes);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if(size == null) {
            return output;
        } else if(size > output.length()) {
            return output;
        } else {
            return output.substring(0, size);
        }
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