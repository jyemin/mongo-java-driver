package com.mongodb.internal.connection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.mongodb.MongoCredential.ENVIRONMENT_KEY;

/**
 * Contains all validation logic for OIDC in one location
 */
public final class OidcValidator {

    private static final String TEST_ENVIRONMENT = "test";
    private static final String AZURE_ENVIRONMENT = "azure";
    private static final String GCP_ENVIRONMENT = "gcp";
    private static final String K8S_ENVIRONMENT = "k8s";
    private static final List<String> IMPLEMENTED_ENVIRONMENTS = Arrays.asList(
            AZURE_ENVIRONMENT, GCP_ENVIRONMENT, K8S_ENVIRONMENT, TEST_ENVIRONMENT);
    private static final List<String> USER_SUPPORTED_ENVIRONMENTS = Arrays.asList(
            AZURE_ENVIRONMENT, GCP_ENVIRONMENT, K8S_ENVIRONMENT);


    private OidcValidator() {
    }

    public static void validateOidcCredentialConstruction(
            final String source,
            final Map<String, Object> mechanismProperties) {

        if (!"$external".equals(source)) {
            throw new IllegalArgumentException("source must be '$external'");
        }

        Object environmentName = mechanismProperties.get(ENVIRONMENT_KEY.toLowerCase());
        if (environmentName != null) {
            if (!(environmentName instanceof String) || !IMPLEMENTED_ENVIRONMENTS.contains(environmentName)) {
                throw new IllegalArgumentException(ENVIRONMENT_KEY + " must be one of: " + USER_SUPPORTED_ENVIRONMENTS);
            }
        }
    }

}
