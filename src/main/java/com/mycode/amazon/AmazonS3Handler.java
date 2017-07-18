package com.mycode.amazon;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.io.ByteStreams;
import org.apache.http.util.Args;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class AmazonS3Handler {

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // INSTANCE PART
    // Thread-safe! It already maintains an internal Connection Pool.
    private AmazonS3 s3client = null;

    private AmazonS3Handler(){

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                "AMAZON_S3_ACCESS_KEY_ID",
                "AMAZON_S3_SECRET_ACCESS_KEY");

        s3client = AmazonS3ClientBuilder.standard().withRegion("eu-central-1")
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    // We only expose what is needed.
    public String storeFile(byte[] input){

        Args.notNull(input, "input");

        InputStream stream = new ByteArrayInputStream(input);
        ObjectMetadata oMeta = new ObjectMetadata();
        oMeta.setContentLength(input.length);
        String uniqueId = UUID.randomUUID().toString();

        s3client.putObject(new PutObjectRequest(
                "AMAZON_S3_BOM_BUCKET_NAME",
                uniqueId,
                stream,
                oMeta));

        // Will be stored in the database, which can be later retrieved back the original file.
        return uniqueId;
    }

    public byte[] fetchFile(String uniqueId) throws IOException {

        Args.notEmpty(uniqueId, "uniqueId");

        S3Object object = s3client.getObject(
                new GetObjectRequest("AMAZON_S3_BOM_BUCKET_NAME", uniqueId));
        InputStream objectData = object.getObjectContent();

        return ByteStreams.toByteArray(objectData);
    }

    public boolean checkExist(String uniqueId){

        Args.notEmpty(uniqueId, "uniqueId");

        return s3client.doesObjectExist("AMAZON_S3_BOM_BUCKET_NAME", uniqueId);
    }

    public void deleteObject(String uniqueId){

        Args.notEmpty(uniqueId, "uniqueId");

        if (checkExist(uniqueId)){
            s3client.deleteObject(new DeleteObjectRequest("AMAZON_S3_BOM_BUCKET_NAME", uniqueId));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STATIC PART
    public static AmazonS3Handler as3Handler;

    public static AmazonS3Handler getInstance(){

        if (as3Handler == null){
            as3Handler = new AmazonS3Handler();
        }

        return as3Handler;
    }

    public static void init(){

        if (as3Handler == null){
            // Can take quite a few seconds to initialize.
            as3Handler = new AmazonS3Handler();
        }

    }
}
