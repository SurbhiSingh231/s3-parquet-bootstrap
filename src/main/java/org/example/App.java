package org.example;

import org.example.parquet.ParquetFileGenerator;
import org.example.s3.S3Service;
import org.example.s3.S3Provider;

public class App {
    public static void main(String []args) {
        S3Service s3Service =  new S3Service(new S3Provider().getS3Client());
        SecretFileProcessor secretFileProcessor = new SecretFileProcessor(s3Service, new ParquetFileGenerator());
        secretFileProcessor.startProcessing();
    }
}
