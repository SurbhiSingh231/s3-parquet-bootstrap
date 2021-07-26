package org.example.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.example.Constants.FILE_FILTER_FORMAT;

public class ParquetFileGenerator {

    public List<File> getParquetFilesContaining(InputStream input, String substring) {
        ZipInputStream in = new ZipInputStream(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line;
        ZipEntry entry;
        List<File> outputFiles = new ArrayList<>();
        try {
            while ((entry = in.getNextEntry()) != null) { // loop through each file within the zip
                if (!entry.getName().endsWith(FILE_FILTER_FORMAT)){
                    continue;
                }
                List<List<String>> data = new ArrayList<>();
                while ((line = reader.readLine()) != null) { // loop through each line
                    if (line.contains(substring)) {
                        createDataForParquetFile(data, line);
                    }
                }
                if (!data.isEmpty()) {
                    String outputFilePath = "src/main/resources/output" + "/" + entry.getName().replace(FILE_FILTER_FORMAT, "") + ".parquet";
                    File outputParquetFile = new File(outputFilePath);
                    CustomParquetWriter writer = getParquetWriter(getSchemaForParquetFile(), outputParquetFile);
                    writeIntoParquetFormat(data, writer);
                    outputFiles.add(outputParquetFile);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return outputFiles;

    }

    private void writeIntoParquetFormat(List<List<String>> data, CustomParquetWriter writer) {
        try {
            for (List<String> column : data) {
                writer.write(column);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void createDataForParquetFile(List<List<String>> data, String line) {

        List<String> parquetFileItem1 = new ArrayList<>();
        parquetFileItem1.add(line);

        data.add(parquetFileItem1);

    }

    private MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File("src/main/resources/schemas/data.schema");
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }


    private CustomParquetWriter getParquetWriter(MessageType schema, File outputFile) throws IOException {
        Path path = new Path(outputFile.toURI().toString());
        return new CustomParquetWriter(
                path, schema, false, CompressionCodecName.SNAPPY
        );
    }
}
