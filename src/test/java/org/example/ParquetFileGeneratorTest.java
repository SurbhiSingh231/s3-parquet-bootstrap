package org.example;

import org.example.parquet.ParquetFileGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetFileGeneratorTest {
    private ParquetFileGenerator parquetFileGenerator;

    @BeforeEach
    public void setUp(){
        parquetFileGenerator = new ParquetFileGenerator();
    }

    @Test
    public void shouldNotCreateParquetFormatFileIfNoSubstringFoundInZipFileProvided() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream("src/test/resources/CsvTestFileWithoutEllipsis.zip");
        List<File> files = parquetFileGenerator.getParquetFilesContaining(inputStream, "ellipsis");
        assertEquals(0, files.size());
    }

    @Test
    public void shouldNotCreateParquetFormatFileIfEmptyZipFileProvided() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream("src/test/resources/CsvTestFileEmpty.zip");
        List<File> files = parquetFileGenerator.getParquetFilesContaining(inputStream, "test");
        assertEquals(0, files.size());
    }

    @Test
    public void shouldCreateParquetFormatFile() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream("src/test/resources/TestCsvFile.zip");
        List<File> files = parquetFileGenerator.getParquetFilesContaining(inputStream, "ellipsis");
        assertEquals(1, files.size());
        assertEquals( "TestCsvFile.parquet", files.get(0).getName());
        files.get(0).deleteOnExit();
    }

    @Test
    public void shouldNotCreateParquetFileForAnyOtherFileFormatThanCSV() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream("src/test/resources/NotCsvFormatFile.zip");
        List<File> files = parquetFileGenerator.getParquetFilesContaining(inputStream, "ellipsis");
        assertEquals(0, files.size());
    }

}
