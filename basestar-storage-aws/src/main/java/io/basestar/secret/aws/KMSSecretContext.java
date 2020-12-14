package io.basestar.secret.aws;

import io.basestar.secret.Secret;
import io.basestar.secret.SecretContext;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsAsyncClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec;

import java.util.concurrent.CompletableFuture;

public class KMSSecretContext implements SecretContext {

    private final KmsAsyncClient client;

    private final EncryptionAlgorithmSpec algorithm;

    private final String keyId;

    @lombok.Builder(builderClassName = "Builder")
    public KMSSecretContext(final KmsAsyncClient client, final String keyId,
                            final EncryptionAlgorithmSpec algorithm) {

        this.client = client;
        this.keyId = keyId;
        this.algorithm = algorithm;
    }

    @Override
    public CompletableFuture<Secret.Encrypted> encrypt(final Secret.Plaintext plaintext) {

        return client.encrypt(EncryptRequest.builder()
                .keyId(keyId)
                .encryptionAlgorithm(algorithm)
                .plaintext(SdkBytes.fromByteArray(plaintext.plaintext()))
                .build()).thenApply(response -> {
            final SdkBytes encrypted = response.ciphertextBlob();
            return Secret.encrypted(encrypted.asByteArray());
        });
    }

    @Override
    public CompletableFuture<Secret.Plaintext> decrypt(final Secret.Encrypted secret) {

        return client.decrypt(DecryptRequest.builder()
                .keyId(keyId)
                .encryptionAlgorithm(algorithm)
                .ciphertextBlob(SdkBytes.fromByteArray(secret.encrypted()))
                .build()).thenApply(response -> {
            final SdkBytes plaintext = response.plaintext();
            return Secret.plaintext(plaintext.asByteArray());
        });
    }
}
