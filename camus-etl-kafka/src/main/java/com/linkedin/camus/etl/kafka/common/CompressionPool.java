package com.linkedin.camus.etl.kafka.common;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

/**
 *
 * Compression Codecs use Decompressor and Compressor instances to do the
 * delagate compression work. Each instance allocates direct memory to spead up
 * memory communication between the native libraries and the java process. Each
 * direc memory allocation will not be automatically released and a release
 * cannot be triggered by the application code. When and how the memory is
 * de-allocated is up to the GC no the JVM version and platform used. This makes
 * the direct allocation of memory very expensive and as a result its better to
 * cache it in a bounded pool.
 * <p/>
 * Thread safety, The Decompressor and Compressor instances are not thread safe,
 * this pool will return an InputStream and OutputStream with a pooled
 * Decompressor or Compressor obtained in a thread safe way.
 * <p/>
 * Pool Sizes: A greater pool size will mean less thread contention but will
 * also mean more memory is allocated.
 * <p/>
 * Non Native Compression Codecs:<br/>
 * Compression Codecs that support non native compression i.e. do not need
 * native libraries and have java alternatives, will not create Compressor(s) or
 * Decompressor(s),<br/>
 * in this case, no resource management is needed and the input and output
 * streams are created directly, ingnoring the pools.
 */
public class CompressionPool {

	private static final Logger LOG = Logger
			.getLogger(CompressionPool.class);

	final LinkedBlockingDeque<Decompressor> decompressorQueue;
	final LinkedBlockingDeque<Compressor> compressorQueue;

	final Map<CompressionInputStream, Decompressor> usedDecompressors = new ConcurrentHashMap<CompressionInputStream, Decompressor>();
	final Map<CompressionOutputStream, Compressor> usedCompressors = new ConcurrentHashMap<CompressionOutputStream, Compressor>();

	final CompressionCodec codec;

	boolean hasDecompressors = false;
	boolean hasCompressors = false;

	/**
	 *
	 * @param codec
	 * @param decompressorPoolSize
	 *            a fixed size Decompressor pool is created with size == this
	 *            value
	 * @param compressorPoolSize
	 *            a fixed size Compressor pool is created with size == this
	 *            value
	 */
	public CompressionPool(CompressionCodec codec,
			int decompressorPoolSize, int compressorPoolSize) {

		this.codec = codec;

		if (codec.createDecompressor() != null) {
			hasDecompressors = true;
			LOG.info("Creating " + decompressorPoolSize + " decompressors"
					+ codec.getClass().getName());
			Decompressor[] decompressors = new Decompressor[decompressorPoolSize];
			for (int i = 0; i < decompressorPoolSize; i++) {
				decompressors[i] = codec.createDecompressor();
			}

			decompressorQueue = new LinkedBlockingDeque<Decompressor>(
					Arrays.asList(decompressors));
		} else {
			decompressorQueue = null;
		}

		if (codec.createCompressor() != null) {
			hasCompressors = true;
			LOG.info("Creating " + compressorPoolSize + " Compressors for "
					+ codec.getClass().getName());
			Compressor[] compressors = new Compressor[compressorPoolSize];
			for (int i = 0; i < compressorPoolSize; i++) {
				compressors[i] = codec.createCompressor();
			}

			compressorQueue = new LinkedBlockingDeque<Compressor>(
					Arrays.asList(compressors));
		} else {
			compressorQueue = null;
		}

	}

	public CompressionInputStream create(InputStream input, long timeout,
			TimeUnit unit) throws IOException, InterruptedException {
		if (hasDecompressors) {
			Decompressor decompressor = decompressorQueue.poll(timeout, unit);

			if (decompressor == null) {
				return null;
			} else {
				CompressionInputStream cin = codec.createInputStream(input,
						decompressor);
				usedDecompressors.put(cin, decompressor);
				return cin;
			}
		} else {
			return codec.createInputStream(input);
		}
	}

	public CompressionOutputStream create(OutputStream output, long timeout,
			TimeUnit unit) throws IOException, InterruptedException {
		if (hasCompressors) {
			Compressor compressor = compressorQueue.poll(timeout, unit);
			if (compressor == null) {
				return null;
			} else {
				CompressionOutputStream cout = codec.createOutputStream(output,
						compressor);
				usedCompressors.put(cout, compressor);
				return cout;
			}
		} else {
			return codec.createOutputStream(output);
		}
	}

	public void closeAndRelease(CompressionInputStream cin) {

		IOUtils.closeQuietly(cin);

		if (hasDecompressors) {
			Decompressor dec = usedDecompressors.remove(cin);
			dec.reset();
			decompressorQueue.offer(dec);
		}

	}

	public void closeAndRelease(CompressionOutputStream cout) {

		try {
			// finish quietly
			cout.finish();
		} catch (IOException ioexp) {
			LOG.error(ioexp.toString(), ioexp);
		}

		IOUtils.closeQuietly(cout);

		if (hasCompressors) {
			Compressor comp = usedCompressors.remove(cout);
			comp.reset();
			compressorQueue.offer(comp);
		}

	}

}