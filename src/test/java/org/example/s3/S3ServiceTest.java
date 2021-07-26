package org.example.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;

import static org.example.Constants.BUCKET_NAME;
import static org.example.Constants.S3_OBJECT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3ServiceTest {

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private File file;

    private S3Service s3Service;

    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectCaptor;

    @Captor
    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @BeforeEach
    public void before() {
        s3Service = new S3Service(amazonS3);
    }

    @Test
    public void shouldDownloadFromCorrectS3Bucket() {
        S3Object s3Object = new S3Object();
        when(amazonS3.getObject(any())).thenReturn(s3Object);
        s3Service.downloadFromS3();
        verify(amazonS3).getObject(getObjectCaptor.capture());
        assertEquals(BUCKET_NAME, getObjectCaptor.getValue().getBucketName());
        assertEquals(S3_OBJECT_KEY, getObjectCaptor.getValue().getKey());
    }

    @Test
    public void shouldUploadToCorrectS3Bucket() {
        File testFile = new File("/testFile");
        s3Service.uploadToS3(testFile);
        verify(amazonS3).putObject(putObjectRequestCaptor.capture());
        assertEquals(BUCKET_NAME, putObjectRequestCaptor.getValue().getBucketName());
        assertEquals(testFile.getName(), putObjectRequestCaptor.getValue().getFile().getName());
    }

    @Test
    public void shouldDeleteFileAfterUpload() {
        s3Service.uploadToS3(file);
        verify(amazonS3).putObject(putObjectRequestCaptor.capture());
        assertEquals(BUCKET_NAME, putObjectRequestCaptor.getValue().getBucketName());
        assertEquals(file.getName(), putObjectRequestCaptor.getValue().getFile().getName());
        verify(file).deleteOnExit();
    }

    @Test
    public void shouldThrowExceptionOnFileUpload() {
        when(amazonS3.putObject(any())).thenThrow(new SdkClientException("exception occurred"));
        Exception exception = assertThrows(SdkClientException.class, () -> {
            s3Service.uploadToS3(file);
        });
        assertEquals(exception.getMessage(), "exception occurred");
        verify(file, times(0)).deleteOnExit();
    }

    @Test
    public void shouldThrowExceptionOnFileDownload() {
        when(amazonS3.getObject(any())).thenThrow(new SdkClientException("exception occurred"));
        Exception exception = assertThrows(SdkClientException.class, () -> {
            s3Service.downloadFromS3();;
        });
        assertEquals(exception.getMessage(), "exception occurred");
    }

}
