package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.util.FileSystems;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class DataDb {

    private final Path datadb;

    public DataDb(String appdir) {
        datadb = new Path(appdir, "datadb");

        ensureRequiredPathExists(datadb.toString());
    }

    private static void ensureRequiredPathExists(String p) {
        try {
            if (!Files.exists(Paths.get(p))) {
                Files.createDirectories(Paths.get(p));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path getDatadb() {
        return datadb;
    }

    public void install(Path nw) throws IOException {
        if (FileSystems.exists(datadb))
            FileSystems.deleteDirectory(datadb);

        FileSystems.moveToLocalFile(nw, datadb);
    }

    public void merge(Path ... ins) {
        Path rOut = null;
        try {
            rOut = FileSystems.randomDirectory("datadb-");

            Job job = new DataDbMergeJob(rOut, datadb, ins).create();
            job.waitForCompletion(true);

            install(rOut);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rOut != null && FileSystems.exists(rOut)) {
                    FileSystems.deleteDirectory(rOut);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
