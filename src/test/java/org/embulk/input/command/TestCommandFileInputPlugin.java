/*
 * Copyright 2015 Sadayuki Furuhashi, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.input.command;

import static org.embulk.input.command.CommandFileInputPlugin.buildShell;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.embulk.EmbulkTestRuntime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
