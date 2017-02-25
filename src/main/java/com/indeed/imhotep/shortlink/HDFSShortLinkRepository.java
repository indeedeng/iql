package com.indeed.imhotep.shortlink;

import com.indeed.imhotep.web.KerberosUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.springframework.core.env.PropertyResolver;

import java.io.IOException;

/**
 * @author jack@indeed.com (Jack Humphrey)
 */
public class HDFSShortLinkRepository implements ShortLinkRepository {
    static final Logger log = Logger.getLogger(HDFSShortLinkRepository.class);

    private boolean enabled;
    private FileSystem hdfs;
    private Path linkPath;
    private boolean worldWritable;

    public HDFSShortLinkRepository(final PropertyResolver props) {
        enabled = true;
        try {
            kerberosLogin(props);

            linkPath = new Path(props.getProperty("shortlink.hdfs.path", String.class));
            hdfs = linkPath.getFileSystem(new Configuration());
            log.info("Short linking will use HDFS path: " + linkPath);
            worldWritable = props.getProperty("shortlink.hdfs.worldwritable", Boolean.class, true);

            if(!hdfs.exists(linkPath)) {
                hdfs.mkdirs(linkPath);
                if(worldWritable) {
                    hdfs.setPermission(linkPath, FsPermission.valueOf("-rwxrwxrwx"));
                }
            }

            log.info("HDFSShortLinkRepository initialized");
        } catch (Exception e) {
            log.info("Failed to initialize the HDFS client. Shortlinking disabled.", e);
            enabled = false;
        }
    }


    private void kerberosLogin(PropertyResolver props) {
        try {
            KerberosUtils.loginFromKeytab(props.getProperty("kerberos.principal"), props.getProperty("kerberos.keytab"));
        } catch (IOException e) {
            log.error("Failed to log in to Kerberos", e);
        }
    }

    @Override
    public boolean mapShortCode(String code, String paramString) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Shortlink feature disabled");
        }
        final Path filePath = new Path(linkPath, code);
        final Path tempPath = new Path(filePath.toString() + "." + (System.currentTimeMillis() % 100000) + ".tmp");

        FSDataOutputStream out = hdfs.create(tempPath);
        out.writeUTF(paramString);
        out.close();
        hdfs.rename(tempPath, filePath);

        return true;
    }

    @Override
    public String resolveShortCode(String shortCode) throws IOException {
        if(!enabled) {
            throw new IllegalStateException("Shortlink feature disabled");
        }
        Path linkFile = new Path(linkPath, shortCode);
        FSDataInputStream in = hdfs.open(linkFile);
        final String query = in.readUTF();
        in.close();
        return query;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }
}
