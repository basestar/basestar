//package io.basestar.schema.secret.aws;
//
//import io.basestar.schema.secret.Secret;
//import io.basestar.schema.secret.SecretContext;
//import software.amazon.awssdk.core.SdkBytes;
//import software.amazon.awssdk.services.kms.KmsAsyncClient;
//import software.amazon.awssdk.services.kms.KmsClient;
//import software.amazon.awssdk.services.kms.model.DecryptRequest;
//import software.amazon.awssdk.services.kms.model.EncryptRequest;
//import software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec;
//
//import java.util.concurrent.CompletableFuture;
//
//public class KMSSecretContext implements SecretContext {
//
//    private final KmsClient client;
//
//    private final EncryptionAlgorithmSpec algorithm;
//
//    private final String keyId;
//
//    @lombok.Builder(builderClassName = "Builder")
//    public KMSSecretContext(final KmsClient client, final String keyId,
//                            final EncryptionAlgorithmSpec algorithm) {
//
//        this.client = client;
//        this.keyId = keyId;
//        this.algorithm = algorithm;
//    }
//
//    @Override
//    public Secret encrypt(final byte[] plaintext) {
//
//        return client.encrypt(EncryptRequest.builder()
//                .keyId(keyId)
//                .encryptionAlgorithm(algorithm)
//                .plaintext(SdkBytes.fromByteArray(plaintext))
//                .build())
//                .thenApply(v -> {
//                    final SdkBytes encrypted = v.ciphertextBlob();
//                    return new Secret(encrypted.asByteArray());
//                });
//    }
//
//    @Override
//    public byte[] decrypt(final Secret secret) {
//
//        return client.decrypt(DecryptRequest.builder()
//                .keyId(keyId)
//                .encryptionAlgorithm(algorithm)
//                .ciphertextBlob(SdkBytes.fromByteArray(secret.encrypted()))
//                .build())
//                .thenApply(v -> {
//                    final SdkBytes plaintext = v.plaintext();
//                    return plaintext.asByteArray();
//                });
//    }
//}
