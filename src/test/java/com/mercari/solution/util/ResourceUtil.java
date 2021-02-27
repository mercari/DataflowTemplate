package com.mercari.solution.util;

import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ResourceUtil {

    public static String getResourceFileAsString(final String path) throws IOException {
        final URL uri = ClassLoader.getSystemResource(path);
        final File file = new File(uri.getPath());
        return getFileAsString(file);
    }

    public static byte[] getResourceFileAsBytes(final String path) {
        try(final InputStream is = Thread.currentThread().getContextClassLoader().getSystemResourceAsStream(path)) {
            return IOUtils.toByteArray(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + path, e);
        }
    }

    private static String getFileAsString(final File file) throws IOException {
        try(final FileInputStream fis = new FileInputStream(file);
            final InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            final BufferedReader br = new BufferedReader(isr)) {
            final StringBuilder sb = new StringBuilder();
            br.lines().forEach(line -> sb.append(line + '\n'));
            return sb.toString();
        }
    }

}
