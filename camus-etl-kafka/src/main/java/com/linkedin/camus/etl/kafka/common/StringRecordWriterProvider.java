package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Provides a RecordWriter that uses FSDataOutputStream to write
 * a String record as bytes to HDFS without any reformatting or compession.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {
    private static final int CREATE_OUTPUTSTREAM_TIMEOUT = 60; //in normal conditions, should never be reached

    private static Logger log = Logger.getLogger(StringRecordWriterProvider.class);

    public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
    public static final String DEFAULT_RECORD_DELIMITER    = "";

    protected String recordDelimiter = null;

    private String extension = "";
    private boolean isCompressed = false;
    private CompressionPool compressionPool = null;

    public StringRecordWriterProvider(TaskAttemptContext  context) {
    	Configuration conf = context.getConfiguration();

        if (recordDelimiter == null) {
            recordDelimiter = conf.get(
                ETL_OUTPUT_RECORD_DELIMITER,
                DEFAULT_RECORD_DELIMITER
            );
        }

        isCompressed = FileOutputFormat.getCompressOutput(context);

        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = null;
            String etlOutputCodec = EtlMultiOutputFormat.getEtlOutputCodec(context);
            if ("snappy".equals(etlOutputCodec)) {
                codecClass = SnappyCodec.class;
            } else if ("gzip".equals(etlOutputCodec)) {
                codecClass = GzipCodec.class;
            } else if ("bzip2".equals(etlOutputCodec)) {
                codecClass = BZip2Codec.class;
            } else {
                codecClass = DefaultCodec.class;
            }
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
            int maxConcurrentWriters = EtlMultiOutputFormat.getMaxConcurrentWriters(context);
            compressionPool = new CompressionPool(codec, 0, maxConcurrentWriters);
        }
    }

    // TODO: Make this configurable somehow.
    // To do this, we'd have to make RecordWriterProvider have an
    // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
    @Override
    public String getFilenameExtension() {
        return extension;
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext  context,
            String              fileName,
            CamusWrapper        camusWrapper,
            FileOutputCommitter committer) throws IOException, InterruptedException {

        // If recordDelimiter hasn't been initialized, do so now
        if (recordDelimiter == null) {
            recordDelimiter = context.getConfiguration().get(
                ETL_OUTPUT_RECORD_DELIMITER,
                DEFAULT_RECORD_DELIMITER
            );
        }

        FSDataOutputStream fileOut = getTemporaryOutputFile(context, fileName, committer);

        if (!isCompressed) {
            return new ByteRecordWriter(fileOut, recordDelimiter);
        } else {
            return new ByteRecordWriter(compressionPool.create(fileOut, CREATE_OUTPUTSTREAM_TIMEOUT, TimeUnit.SECONDS), recordDelimiter);
        }

        /*
        // Create a FSDataOutputStream stream that will write to path.
        final FSDataOutputStream writer = path.getFileSystem(context.getConfiguration()).create(path);

        // Return a new anonymous RecordWriter that uses the
        // FSDataOutputStream writer to write bytes straight into path.
        return new RecordWriter<IEtlKey, CamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                String record = (String)data.getRecord() + recordDelimiter;
                writer.write(record.getBytes());
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
            }
        };
        */
    }

    private FSDataOutputStream getTemporaryOutputFile(TaskAttemptContext context,
            String fileName, FileOutputCommitter committer) throws IOException {
        // Get the filename for this RecordWriter.
        Path path = new Path(
            committer.getWorkPath(),
            EtlMultiOutputFormat.getUniqueFile(
                context, fileName, getFilenameExtension()
            )
        );

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        int index = 1;
        while(fs.exists(path)) {
            String suffixedFileName = String.format("%s__subpart-%d-", fileName, index);
            log.warn("Temporary file "  + fileName + " already exists. Creating new file: " + suffixedFileName);
            path = new Path(
                    committer.getWorkPath(),
                    EtlMultiOutputFormat.getUniqueFile(
                            context, suffixedFileName, getFilenameExtension()
                            )
                    );
            index++;
        }

        return fs.create(path, false);
    }

    protected class ByteRecordWriter extends RecordWriter<IEtlKey, CamusWrapper> {
        private OutputStream out;
        private String recordDelimiter;

        public ByteRecordWriter(OutputStream out, String recordDelimiter) {
            this.out = out;
            this.recordDelimiter = recordDelimiter;
        }

        @Override
        public void write(IEtlKey ignore, CamusWrapper value) throws IOException {
            boolean nullValue = value == null;
            if (!nullValue) {
            	String record = (String)value.getRecord() + recordDelimiter;
                out.write(record.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (out instanceof CompressionOutputStream) {
                compressionPool.closeAndRelease((CompressionOutputStream)out);
            }
            out.close();
        }
    }
}
