package com.mercari.solution.util.aws;

import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class S3Util {

    public static S3Client storage(final String accessKey, final String secretKey, final String region) {
        if(accessKey == null || secretKey == null) {
            return S3Client.builder()
                    .region(Region.of(region))
                    .build();
        } else {
            final StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider
                    .create(AwsBasicCredentials.create(accessKey, secretKey));
            return S3Client.builder()
                    .credentialsProvider(staticCredentialsProvider)
                    .region(Region.of(region))
                    .build();
        }
    }

    public static String readString(final String accessKey, final String secretKey, final String region, final String s3Path) {

        final String[] paths = parseS3Path(s3Path);
        final S3Client s3 = storage(accessKey, secretKey, region);
        return readString(s3, paths[0], paths[1]);
    }

    public static byte[] readBytes(final S3Client s3, final String s3Path) {
        final String[] paths = parseS3Path(s3Path);
        return readBytes(s3, paths[0], paths[1]);
    }

    public static void writeBytes(
            final S3Client s3,
            final String s3Path,
            final byte[] content,
            final String type,
            final Map<String, Object> attributes,
            final Map<String, String> metadata) {

        final String[] paths = parseS3Path(s3Path);

        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(paths[0]).key(paths[1]).contentType(type).metadata(metadata);
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
            switch (entry.getKey()) {
                case "storageClass":
                    builder = builder.storageClass((String)entry.getValue());
                case "objectLockMode":
                    builder = builder.objectLockMode((String)entry.getValue());
                case "bucketKeyEnabled":
                    builder = builder.bucketKeyEnabled((Boolean)entry.getValue());
                case "redirectLocation":
                    builder = builder.websiteRedirectLocation((String)entry.getValue());
            }
        }
        final RequestBody body = RequestBody.fromBytes(content);
        s3.putObject(builder.build(), body);
    }

    public static List<S3Object> listFiles(
            final S3Client s3,
            final String s3Path) {

        final String[] paths = parseS3Path(s3Path);
        return listFiles(s3, paths[0], paths[1]);
    }

    public static void copy(final S3Client s3, final String sourcePath, final String destinationPath, final Map<String, Object> attributes) {
        final String[] sourcePaths = parseS3Path(sourcePath);
        final String[] destinationPaths = parseS3Path(destinationPath);
        CopyObjectRequest.Builder builder = CopyObjectRequest.builder()
                .sourceBucket(sourcePaths[0])
                .sourceKey(sourcePaths[1])
                .destinationBucket(destinationPaths[0])
                .destinationKey(destinationPaths[1]);
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
            switch (entry.getKey()) {
                case "storageClass":
                    builder = builder.storageClass((String)entry.getValue());
                case "objectLockMode":
                    builder = builder.objectLockMode((String)entry.getValue());
                case "bucketKeyEnabled":
                    builder = builder.bucketKeyEnabled((Boolean)entry.getValue());
                case "redirectLocation":
                    builder = builder.websiteRedirectLocation((String)entry.getValue());
            }
        }
        s3.copyObject(builder.build());
    }

    public static Schema getAvroSchema(final String s3Path,
                                       final String accessKey, final String secretKey, final String region) {
        final S3Client s3 = storage(accessKey, secretKey, region);
        final String[] paths = parseS3Path(s3Path);
        return getAvroSchema(s3, paths[0], paths[1]);
    }

    public static Schema getAvroSchema(final S3Client s3,
                                       final String s3Path) {
        final String[] paths = parseS3Path(s3Path);
        return getAvroSchema(s3, paths[0], paths[1]);
    }

    public static Schema getAvroSchema(final S3Client s3,
                                       final String bucket,
                                       final S3Object object) {
        return getAvroSchema(s3, bucket, object.key());
    }

    public static Schema getAvroSchema(final S3Client s3, final String bucket, final String object) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(object).build();
        try(final InputStream is = s3.getObject(request);
            final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(is, datumReader)) {
            return dataFileReader.getSchema();
        } catch (Exception e) {
            return null;
        }
    }

    public static Schema getParquetSchema(final S3Client s3,
                                          final String s3Path) {
        final String[] paths = parseS3Path(s3Path);
        return getParquetSchema(s3, paths[0], paths[1]);
    }

    public static Schema getParquetSchema(final S3Client s3,
                                          final String bucket,
                                          final S3Object object) {
        return getParquetSchema(s3, bucket, object.key());
    }

    public static Schema getParquetSchema(final S3Client s3, final String bucket, final String object) {
        final byte[] bytes;
        try {
            bytes = readBytes(s3, bucket, object);
        } catch (Exception e) {
            return null;
        }
        try(final ParquetFileReader f = ParquetFileReader.open(new ParquetStream(bytes))) {
            return new AvroSchemaConverter().convert(f.getFooter().getFileMetaData().getSchema());
        } catch (Exception e) {
            return null;
        }
    }

    public static String getBucketName(String s3Path) {
        if(s3Path == null) {
            throw new IllegalArgumentException("s3Path must not be null");
        }
        if(!s3Path.startsWith("s3://")) {
            throw new IllegalArgumentException("s3Path must start with s3://");
        }
        final String[] paths = s3Path.replaceAll("s3://", "").split("/", 2);
        if(paths.length != 2) {
            throw new IllegalArgumentException("Illegal s3Path: " + s3Path);
        }
        return paths[0];
    }

    private static String[] parseS3Path(String s3Path) {
        if(s3Path == null) {
            throw new IllegalArgumentException("gcsPath must not be null");
        }
        if(!s3Path.startsWith("s3://")) {
            throw new IllegalArgumentException("s3Path must start with s3://");
        }
        final String[] paths = s3Path.replaceAll("s3://", "").split("/", 2);
        if(paths.length != 2) {
            throw new IllegalArgumentException("Illegal gcsPath: " + s3Path);
        }
        return paths;
    }

    private static String readString(final S3Client s3,
                                     final String bucket,
                                     final String object) {

        final byte[] bytes = readBytes(s3, bucket, object);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] readBytes(final S3Client s3, final String bucket, final String object) {
        try {
            return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(object).build()).readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<S3Object> listFiles(final S3Client s3, final String bucket, final String prefix) {
        final String p = prefix.endsWith("*") ? prefix.replace("*", "") : prefix;
        final List<S3Object> objects = new ArrayList<>();
        ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket).prefix(p).build();
        ListObjectsResponse response;
        try {
            do {
                response = s3.listObjects(request);
                final List<S3Object> contents = response.contents();
                objects.addAll(contents);
                request = ListObjectsRequest.builder().bucket(bucket).prefix(p).marker(response.marker()).build();
            } while (response.isTruncated());

            return objects;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class ParquetStream implements InputFile {
        private final byte[] data;

        public class SeekableByteArrayInputStream extends ByteArrayInputStream {

            public SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public void setPos(int pos) {
                this.pos = pos;
            }

            public int getPos() {
                return this.pos;
            }
        }

        public ParquetStream(final byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new ParquetStream.SeekableByteArrayInputStream(this.data)) {

                @Override
                public void seek(long newPos) {
                    ((StorageUtil.ParquetStream.SeekableByteArrayInputStream) this.getStream()).setPos(Long.valueOf(newPos).intValue());
                }

                @Override
                public long getPos() {

                    return Integer.valueOf(((StorageUtil.ParquetStream.SeekableByteArrayInputStream) this.getStream()).getPos()).longValue();
                }
            };
        }
    }

}
