package com.indeed.util.tempfiles;

import com.indeed.imhotep.utils.tempfiles.AbstractTempFiles;
import com.indeed.imhotep.utils.tempfiles.LoggingEventListener;
import com.indeed.imhotep.utils.tempfiles.TempFile;
import com.indeed.imhotep.utils.tempfiles.TempFileType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

public class IQLTempFiles extends AbstractTempFiles<IQLTempFiles.Type> {
    private static final Logger LOGGER = Logger.getLogger(IQLTempFiles.class);
    private static final AtomicReference<IQLTempFiles> INSTANCE = new AtomicReference<>(
            builder()
                    .setEventListener(new LoggingEventListener(Level.WARN, LOGGER))
                    .setRoot(Paths.get(System.getProperty("java.io.tmpdir")))
                    .build()
    );

    public static void recreate(final AbstractTempFiles.Builder<Type, IQLTempFiles> imhotepTempFiles) {
        INSTANCE.set(imhotepTempFiles.build());
    }

    public static IQLTempFiles getInstance() {
        return INSTANCE.get();
    }

    private IQLTempFiles(final Builder<IQLTempFiles.Type, IQLTempFiles> builder) {
        super(builder);
    }

    public static Builder<Type, IQLTempFiles> builder() {
        return new Builder<>(Type.class, IQLTempFiles::new);
    }

    public static TempFile createForIQL1(final String queryHash) throws IOException {
        return getInstance().createTempFile(Type.IQL1_QUERY_CACHE, queryHash);
    }

    public static TempFile createForIQL2(final String queryHash) throws IOException {
        return getInstance().createTempFile(Type.IQL2_QUERY_CACHE, queryHash);
    }

    @AllArgsConstructor
    @Getter
    enum Type implements TempFileType<Type> {
        IQL1_QUERY_CACHE("iql_tmp"),
        IQL2_QUERY_CACHE("query"),
        ;
        private final String identifier;
    }
}
