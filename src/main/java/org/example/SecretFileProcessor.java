package org.example;

import org.example.parquet.ParquetFileGenerator;
import org.example.s3.S3Service;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import static org.example.Constants.FILTER_STRING;

public class SecretFileProcessor {

    private S3Service s3Service;
    private ParquetFileGenerator parquetFileGenerator;


    public SecretFileProcessor(S3Service s3Service, ParquetFileGenerator parquetFileGenerator) {
        this.s3Service = s3Service;
        this.parquetFileGenerator = parquetFileGenerator;
    }

    public void startProcessing() {
        InputStream inputStream = s3Service.downloadFromS3();
        List<File> filesToUpload = parquetFileGenerator.getParquetFilesContaining(inputStream, FILTER_STRING);
        filesToUpload.forEach(file -> s3Service.uploadToS3(file));
    }
}
