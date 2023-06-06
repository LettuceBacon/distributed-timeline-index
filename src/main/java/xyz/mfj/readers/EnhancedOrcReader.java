package xyz.mfj.readers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.impl.ReaderImpl;

import java.util.function.Supplier;

public class EnhancedOrcReader extends ReaderImpl{

    public EnhancedOrcReader(Path path, ReaderOptions options) throws IOException {
        super(path, options);
    }
    
    public Path getPath() {
        return path;
    }
    
    public Configuration getConf() {
        return conf;
    }
    
    public boolean isUseUTCTimestamp() {
        return useUTCTimestamp;
    }
    
    public OrcFile.ReaderOptions getOptions() {
        return options;
    }
    
    public Supplier<FileSystem> getFileSystemSupplier() {
        return super.getFileSystemSupplier();
    }
    
    public FileSystem getFileSystem() throws IOException {
        return super.getFileSystem();
    }
}
