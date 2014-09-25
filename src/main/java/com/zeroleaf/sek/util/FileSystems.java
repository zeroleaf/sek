package com.zeroleaf.sek.util;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;

/**
 * FileSystem 的辅助类. 提供一些常用的方法.
 *
 * @author zeroleaf
 */
public class FileSystems {

    private static FileSystem fs;

    static {
        try {
            fs = FileSystem.get(SekConf.getInstance());
        } catch (IOException e) {
            System.exit(1);
        }
    }

    public static void moveToLocalFile(Path src, Path dest) throws IOException {
        fs.moveToLocalFile(src, dest);
    }

    public static void deleteDirectory(Path dir) throws IOException {
        fs.delete(dir, true);
    }

    /**
     * 在当前文件夹下创建一个文件夹并返回对应的 Path 对象.
     *
     * 文件夹的名字是随机的, 但带有指定的前缀.
     *
     * @param prefix 文件夹名称的前缀.
     * @return 该文件夹对应的 Path 实例.
     */
    public static Path randomDirectory(String prefix) {
        return new Path(prefix + UUID.randomUUID().toString());
    }


    private FileSystems() {}
}
