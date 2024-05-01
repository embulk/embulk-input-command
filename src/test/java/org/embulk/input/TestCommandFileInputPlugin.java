package org.embulk.input;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.embulk.test.EmbulkTestRuntime;
import static org.embulk.input.CommandFileInputPlugin.buildShell;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestCommandFileInputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testShell() {
        if (System.getProperty("os.name").indexOf("Windows") >= 0) {
            assertEquals(Collections.unmodifiableList(Arrays.asList("PowerShell.exe", "-Command")), buildShell());
        }
        else {
            assertEquals(Collections.unmodifiableList(Arrays.asList("sh", "-c")), buildShell());
        }
    }
}
