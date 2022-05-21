package com.mercari.solution.util.gcp;

import org.junit.Assert;
import org.junit.Test;

public class SecretManagerUtilTest {

    @Test
    public void testIsSecretName() {
        final String secretName1 = "projects/my-project1/secrets/my-secrets_/versions/1";
        Assert.assertTrue(SecretManagerUtil.isSecretName(secretName1));
        final String secretName2 = "/projects/my-project1/secrets/my-secrets_/versions/1";
        Assert.assertFalse(SecretManagerUtil.isSecretName(secretName2));
        final String secretName3 = "projects//secrets/my-secrets_/versions/1";
        Assert.assertFalse(SecretManagerUtil.isSecretName(secretName3));
        final String secretName4 = "projects/my-project1/secrets//versions/1";
        Assert.assertFalse(SecretManagerUtil.isSecretName(secretName4));
        final String secretName5 = "projects/my-project1/secrets/my-secrets/versions/";
        Assert.assertFalse(SecretManagerUtil.isSecretName(secretName5));
        final String secretName6 = "projects/my-project1/secrets/my-secrets/versions/a";
        Assert.assertFalse(SecretManagerUtil.isSecretName(secretName6));
        final String secretName7 = "projects/my-project1/secrets/my-secrets/versions/latest";
        Assert.assertTrue(SecretManagerUtil.isSecretName(secretName7));
    }

}
