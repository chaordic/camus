package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;

public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object>
{
  private static Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);

  private TaskAttemptContext context;
  private Writer errorWriter = null;
  private String currentTopic = "";
  private long beginTimeStamp = 0;

  private RecordWriterCache dataWriters;
  private EtlMultiOutputCommitter committer;

  public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer) throws IOException,
      InterruptedException
  {
    this.context = context;
    this.committer = committer;
    errorWriter =
        SequenceFile.createWriter(FileSystem.get(context.getConfiguration()),
                                  context.getConfiguration(),
                                  new Path(committer.getWorkPath(),
                                           EtlMultiOutputFormat.getUniqueFile(context,
                                                                              EtlMultiOutputFormat.ERRORS_PREFIX,
                                                                              "")),
                                  EtlKey.class,
                                  ExceptionWritable.class);

    if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1)
    {
      int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
      beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
    }
    else
    {
      beginTimeStamp = 0;
    }
    this.dataWriters = new RecordWriterCache(EtlMultiOutputFormat.getMaxConcurrentWriters(context));
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    for (String w : dataWriters.keySet())
    {
      dataWriters.get(w).close(context);
    }
    errorWriter.close();
  }

  @Override
  public void write(EtlKey key, Object val) throws IOException,
      InterruptedException
  {
    if (val instanceof CamusWrapper<?>)
    {
      if (key.getTime() < beginTimeStamp)
      {
        // ((Mapper.Context)context).getCounter("total",
        // "skip-old").increment(1);
        committer.addOffset(key);
      }
      else
      {
        if (!key.getTopic().equals(currentTopic))
        {
          for (RecordWriter<IEtlKey, CamusWrapper> writer : dataWriters.values())
          {
            writer.close(context);
          }
          dataWriters.clear();
          currentTopic = key.getTopic();
        }

        committer.addCounts(key);
        CamusWrapper value = (CamusWrapper) val;
        String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
        if (!dataWriters.containsKey(workingFileName))
        {
          dataWriters.put(workingFileName, getDataRecordWriter(context, workingFileName, value));
        }
        dataWriters.get(workingFileName).write(key, value);
      }
    }
    else if (val instanceof ExceptionWritable)
    {
      committer.addOffset(key);
      System.err.println(key.toString());
      System.err.println(val.toString());
      errorWriter.append(key, (ExceptionWritable) val);
    }
  }

  private class RecordWriterCache extends LinkedHashMap<String, RecordWriter<IEtlKey, CamusWrapper>>
  {
    private int maxCapacity;

    public RecordWriterCache(int maxCapacity)
    {
      super(16, 0.75F, true);
      this.maxCapacity = maxCapacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, RecordWriter<IEtlKey, CamusWrapper>> eldest)
    {
      boolean isFull = maxCapacity != -1 && size() >= this.maxCapacity;
      if (isFull) {
        RecordWriter<IEtlKey,CamusWrapper> recordWriter = eldest.getValue();
        try {
          log.info("Record writer cache is full with " + maxCapacity + " entries. Closing and removing LRU record writer: " + eldest.getKey());
          recordWriter.close(context);
        } catch (Exception e) {
          log.error("Error while closing expired record writer from cache: " + eldest.getKey(), e);
        }
      }
      return isFull;
    }
  }

  private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context,
                                                                  String fileName,
                                                                  CamusWrapper value) throws IOException,
      InterruptedException
  {
    RecordWriterProvider recordWriterProvider = null;
    try
    {
      //recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
      Class<RecordWriterProvider> rwp = EtlMultiOutputFormat.getRecordWriterProviderClass(context);
      Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
      recordWriterProvider = crwp.newInstance(context);
    }
    catch (InstantiationException e)
    {
      throw new IllegalStateException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new IllegalStateException(e);
    }
    catch (Exception e)
    {
        throw new IllegalStateException(e);
    }
    return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer);
  }
}
