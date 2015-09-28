package com.nitro.clients.kafka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

class TestUtils {

    private static final Random RANDOM = new Random();

    private TestUtils() {
    }

    public static File constructTempDir(final String dirPrefix) {
        final File file = new File(
                System.getProperty("java.io.tmpdir"),
                dirPrefix + RANDOM.nextInt(10000000)
        );
        if (!file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }

    public static boolean deleteFile(final File path) throws FileNotFoundException {
        if (!path.exists()) {
            throw new FileNotFoundException(path.getAbsolutePath());
        }
        boolean ret = true;
        if (path.isDirectory()) {
            final File[] fis = path.listFiles();
            if(fis != null){
                for (final File f : fis) {
                    ret = ret && deleteFile(f);
                }
            }
        }
        return ret && path.delete();
    }
}
