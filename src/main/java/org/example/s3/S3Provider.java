package org.example.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import static org.example.Constants.*;

public class S3Provider {

    private AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(new AWSCredentials() {
        @Override
        public String getAWSAccessKeyId() {
            return ACCESS_KEY_ID;
        }

        @Override
        public String getAWSSecretKey() {
            return ACCESS_KEY_SECRETS;
        }
    });

    public AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(CLIENT_REGION)
                .withCredentials(awsStaticCredentialsProvider)
                .build();
    }

}
