package com.oracle.cloudsql.streamingfacade.objectstore;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.Bucket;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.*;
import com.oracle.bmc.objectstorage.responses.CreateBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;


@Slf4j
public class ObjectStoreCursorStorageImpl implements ICursorStorage {

    private final ObjectStorage client;
    private final String compartmentId;
    private final Bucket bucket;
    private final String namespace;
    private static final String BUCKET_NAME = "csql-streaming";

    public ObjectStoreCursorStorageImpl(Region region, String compartmentId) {
        this.compartmentId = compartmentId;
        log.info("Get instance provider");
        final InstancePrincipalsAuthenticationDetailsProvider provider = InstancePrincipalsAuthenticationDetailsProvider.builder().build();
        log.info("Create client");
        this.client = new ObjectStorageClient(provider);
        log.info("set region to: "+region);
        client.setRegion(region);
        log.info("get namespace");
        this.namespace = getNamespace();
        log.info("namespace: "+namespace);
        System.out.println("namespace:"+namespace);
        try {
            log.info("Creating bucket: "+BUCKET_NAME);
            CreateBucketResponse result =  client.createBucket(CreateBucketRequest.builder()
                    .namespaceName(namespace)
                    .createBucketDetails(CreateBucketDetails.builder()
                            .name(BUCKET_NAME)
                            .compartmentId(compartmentId)
                            .build())
                    .build());
            log.info(result.toString());
            System.out.println(result);
        }
        catch (BmcException ex) {
            log.warn(ex.getMessage(), ex);
            System.out.println(ex.getMessage());
            int code = ex.getStatusCode();
            log.warn("code="+code);
            System.out.println("code="+code);
        }
        finally {
            this.bucket = null;
//            GetBucketResponse bucketResponse = client.getBucket(
//                    GetBucketRequest.builder()
//                        .namespaceName(namespace)
//                        .bucketName(BUCKET_NAME)
//                        .build());
//            this.bucket = bucketResponse.getBucket();
        }

    }

    private String getNamespace() {
        GetNamespaceResponse namespaceResponse =
                client.getNamespace(
                        GetNamespaceRequest.builder()
                        .compartmentId(compartmentId)
                        .build());
        return namespaceResponse.getValue();
    }

    @Override
    public String get(String clientId, String streamId) {
//        if(bucket==null) {
//            log.error("bucket is null");
//            return null;
//        }

        try {
            GetObjectRequest gor = GetObjectRequest.builder()
                    .namespaceName(this.namespace)
                    .bucketName(BUCKET_NAME)
                    .objectName(getObjectName(clientId, streamId))
                    .build();

            GetObjectResponse response = client.getObject(gor);
            String result = new BufferedReader(new InputStreamReader(response.getInputStream()))
                    .lines().collect(Collectors.joining("\n"));

            return result;
        }
        catch (BmcException ex) {
            return null;
        }

    }

    @Override
    public void store(String clientId, String streamId, String serializedCursor) {
//        if(bucket==null) {
//            log.error("bucket is null");
//            return;
//        }
//        if(bucket.getIsReadOnly()) {
//            log.error("bucket is read-only");
//        }
        UploadConfiguration uploadConfiguration =
                UploadConfiguration.builder()
                        .allowMultipartUploads(true)
                        .allowParallelUploads(true)
                        .build();

        UploadManager uploadManager = new UploadManager(client, uploadConfiguration);

        PutObjectRequest request = PutObjectRequest.builder()
                .namespaceName(namespace)
                .bucketName(BUCKET_NAME)
                .objectName(getObjectName(clientId, streamId))
                .contentType(null)
                .contentLanguage(null)
                .contentEncoding("UTF-8")
                .opcMeta(null)
                .build();

        Path tempPath;
        try {
            tempPath = Files.createTempFile("csql", "temp");
            File tmpFile = tempPath.toFile();
            com.google.common.io.Files.write(serializedCursor.getBytes(Charset.forName("UTF-8")), tmpFile);
            UploadManager.UploadRequest uploadDetails =
                    UploadManager.UploadRequest.builder(tmpFile).allowOverwrite(true).build(request);
            UploadManager.UploadResponse response = uploadManager.upload(uploadDetails);
            System.out.println(response);
            tmpFile.delete();

        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    private String getObjectName(String clientId, String streamId) {
        return clientId+"/"+streamId;
    }
}
