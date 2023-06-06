package xyz.mfj.readers;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public interface StripeReader extends Closeable{
    /**
     * Read the next row batch. The size of the batch to read cannot be
     * controlled by the callers. Caller need to look at
     * VectorizedRowBatch.size of the returned object to know the batch
     * size read.
     * @param batch a row batch object to read into
     * @return were more rows available to read?
     * @throws java.io.IOException
     * @since 1.1.0
     */
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException;
  
    
    /**
     * Release the resources associated with the given reader.
     * @throws java.io.IOException
     * @since 1.1.0
     */
    @Override
    void close() throws IOException;
    
}
