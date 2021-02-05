package org.hackathon;

import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.util.logging.ClientLogger;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.implementation.Constants;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class UploadDirectoryAsync {

    private static final ClientLogger LOGGER = new ClientLogger(UploadDirectoryAsync.class);

    private static Map<String, String> directoryMetadata  = new HashMap<>();

    public static void main(String[] args) throws IOException {
        directoryMetadata.put(Constants.HeaderConstants.DIRECTORY_METADATA_KEY, "true");

        // Setup code
        String accountName = "";
        String accountKey = "";
        String absoluteDirectoryPath = "";

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
        String containerName = "myjavacontaineruploaddirasync" + System.currentTimeMillis();
        BlobServiceAsyncClient storageClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .httpLogOptions(new HttpLogOptions().addAllowedHeaderName("x-ms-request-id").addAllowedHeaderName("x-ms-client-request-id").setLogLevel(HttpLogDetailLevel.HEADERS))
                .buildAsyncClient();
        BlobContainerAsyncClient containerClient = storageClient.getBlobContainerAsyncClient(containerName);
        containerClient.create().block();

        // Upload directory
        Path myDir = Paths.get(absoluteDirectoryPath);

        uploadDirectoryRecursive(myDir, myDir.toFile().getName(), containerClient)
                .blockLast();

        // Cleanup code
        containerClient.delete().block();
    }

    private static Flux<Void> uploadDirectoryRecursive(Path p, String parentName, BlobContainerAsyncClient parent) throws IOException {
        return Mono.just(parentName)
                .flatMap(name -> uploadDirectory(parent.getBlobAsyncClient(name)))
                .thenMany(
                        Flux.fromStream(Files.list(p))
                                .flatMap(path -> Mono.just(path.toFile().getName())
                                        .map(name -> "".equals(parentName) ? name : parentName + "/" + name)
                                        .flatMapMany(name -> {
                                                    if (path.toFile().isDirectory()) {
                                                        try {
                                                            return uploadDirectoryRecursive(path, name, parent);
                                                        } catch (IOException e) {
                                                            LOGGER.logThrowableAsError(e);
                                                        }
                                                        return Flux.empty();
                                                    } else {
                                                        return uploadFile(path, parent.getBlobAsyncClient(name))
                                                                .flux();
                                                    }
                                                }
                                        )));
    }

    private static Mono<Void> uploadDirectory(BlobAsyncClient client) {
        return client.upload(Flux.empty(), null)
                .then(client.setMetadata(directoryMetadata));
    }

    private static Mono<Void> uploadFile(Path fileName, BlobAsyncClient client) {
        return client.uploadFromFile(fileName.toAbsolutePath().toString());
    }
}
