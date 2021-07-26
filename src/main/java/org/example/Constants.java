package org.example;

import com.amazonaws.regions.Regions;

public class Constants {
    public static final String BUCKET_NAME = "candidate-54-s3-bucket";
    public static final String ACCESS_KEY_ID = "<PLACE_AWS_ACCESS_KEY_ID_HERE>>";
    public static final String ACCESS_KEY_SECRETS = "<PLACE_ACCESS_KEY_SECRETS_HERE>";
    public static final String S3_OBJECT_KEY = "data.zip";
    public static final Regions CLIENT_REGION = Regions.AP_SOUTHEAST_2;
    public static final String FILTER_STRING = "ellipsis";
    public static final String FILE_FILTER_FORMAT = ".csv";
}
