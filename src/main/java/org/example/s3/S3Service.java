package org.example.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;

import static org.example.Constants.BUCKET_NAME;
import static org.example.Constants.S3_OBJECT_KEY;

public class S3Service {
    private final AmazonS3 s3Client;

    public S3Service(AmazonS3 amazonS3) {
        this.s3Client = amazonS3;
    }

    public InputStream downloadFromS3(){
        S3Object fullObject;
        try {

            System.out.println("Downloading an object");
            //fullObject = s3Client.listObjects(bucketName));
            fullObject = s3Client.getObject(new GetObjectRequest(BUCKET_NAME, S3_OBJECT_KEY));
            return fullObject.getObjectContent();

        } catch (SdkClientException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void uploadToS3(File file) {
        try {
            s3Client.putObject(new PutObjectRequest(BUCKET_NAME, file.getName(), file));
            System.out.println("Uploading success for file: "+ file.getName());
            file.deleteOnExit();

        } catch (SdkClientException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }

    }
}
