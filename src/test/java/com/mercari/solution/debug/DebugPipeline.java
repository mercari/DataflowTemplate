package com.mercari.solution.debug;

import com.mercari.solution.FlexPipeline;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class DebugPipeline {

    @Test
    public void debugPipeline() throws Exception {
        // Enable it to check the operation during development.

        //runPipeline();
    }

    private void runPipeline() throws Exception {
        final String project = "myproject";
        // Set path at src/test/resources
        final String templateFilePath = "debug/myconfig.json";

        final URI uri = getResourceURI(templateFilePath);

        final List<String> pipelineArgs = Arrays.asList(
                "--project=" + project,
                "--config=" + Paths.get(uri).toAbsolutePath(),
                "--streaming=false"
        );

        final String[] args = pipelineArgs.toArray(new String[pipelineArgs.size()]);
        FlexPipeline.main(args);
    }

    private URI getResourceURI(final String path) {
        try {
            return Thread.currentThread().getContextClassLoader().getResource(path).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
