package org.example;

import com.amazonaws.util.StringInputStream;
import org.example.s3.S3Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.example.Constants.FILTER_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecretFileProcessorTest {

    @Mock
    private S3Service s3Service;

    @Mock
    private ParquetFileGenerator parquetFileGenerator;

    private SecretFileProcessor secretFileProcessor;

    @Captor
    ArgumentCaptor<InputStream> inputStreamArgumentCaptor;

    @Captor
    ArgumentCaptor<String> stringArgumentCaptor;

    @BeforeEach
    public void setUp(){
        secretFileProcessor = new SecretFileProcessor(s3Service, parquetFileGenerator);
    }


    @Test
    public void shouldProcess() throws UnsupportedEncodingException {
        InputStream inputStream = new StringInputStream("testInputStream");
        List<File> files = new ArrayList<>();
        File testFile = new File("test");
        files.add(testFile);
        when(s3Service.downloadFromS3()).thenReturn(inputStream);
        when(parquetFileGenerator.getParquetFilesContaining(any(), any())).thenReturn(files);
        secretFileProcessor.startProcessing();

        verify(s3Service).downloadFromS3();
        verify(parquetFileGenerator).getParquetFilesContaining(inputStreamArgumentCaptor.capture(), stringArgumentCaptor.capture());
        verify(s3Service).uploadToS3(testFile);
        assertEquals(FILTER_STRING, stringArgumentCaptor.getValue());
    }

}
