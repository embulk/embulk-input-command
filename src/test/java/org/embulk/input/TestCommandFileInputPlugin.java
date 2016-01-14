package org.embulk.input;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.embulk.EmbulkTestRuntime;
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
            assertEquals(ImmutableList.of("PowerShell.exe", "-Command"), buildShell());
        }
        else {
            assertEquals(ImmutableList.of("sh", "-c"), buildShell());
        }
    }
}
