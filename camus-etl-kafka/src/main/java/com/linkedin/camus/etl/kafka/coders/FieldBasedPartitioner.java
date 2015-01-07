package com.linkedin.camus.etl.kafka.coders;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class FieldBasedPartitioner extends Partitioner {

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    //protected DateTimeZone outputDateTimeZone = null;
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {

        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        Long datePartition = DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());

        StringBuilder partitionElements = new StringBuilder(datePartition.toString());
        String[] partitionFields = context.getConfiguration().get(FieldBasedPartitionerMessageDecoder.CAMUS_MESSAGE_PARTITION_FIELDS, "").split(",");
        for (String partitionField : partitionFields) {
            Writable partitionElement = key.getPartitionMap().get(new Text(partitionField));
            if (partitionElement != null) {
                partitionElements.append("_").append(partitionElement.toString());
            }
        }

        return partitionElements.toString();
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");

        StringTokenizer partitionElements = new StringTokenizer(encodedPartition, "_");
        DateTime bucket = new DateTime(Long.valueOf(partitionElements.nextToken()));
        sb.append(bucket.toString(outputDateFormatter));

        while (partitionElements.hasMoreTokens()) {
            sb.append("/").append(partitionElements.nextToken());
        }

        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf)
    {
        if (conf != null){
        	outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "America/Los_Angeles")));
        }

        super.setConf(conf);
    }
}
