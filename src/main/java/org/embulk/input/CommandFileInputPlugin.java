package org.embulk.input;

import java.util.List;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.IOException;
import org.slf4j.Logger;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Throwables;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;

public class CommandFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("command")
        public String getCommand();

        @Config("pipe")
        @ConfigDefault("\"stdout\"")
        public String getPipe();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    public static final List<String> SHELL = ImmutableList.of(
        // TODO use ["PowerShell.exe", "-Command"] on windows?
        "sh", "-c"
    );

    private final Logger logger = Exec.getLogger(getClass());

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        switch (task.getPipe()) {
        case "stdout":
            break;
        case "stderr":
            break;
        default:
            throw new ConfigException(String.format(
                        "Unknown 'pipe' option '%s'. It must be either 'stdout' or 'stderr'", task.getPipe()));
        }

        return resume(task.dump(), 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int processorCount,
            FileInputPlugin.Control control)
    {
        control.run(taskSource, processorCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int processorCount,
            List<CommitReport> successCommitReports)
    {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int processorIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        List<String> cmdline = new ArrayList<String>();
        cmdline.addAll(SHELL);
        cmdline.add(task.getCommand());

        logger.info("Running command {}", cmdline);

        ProcessBuilder builder = new ProcessBuilder(cmdline.toArray(new String[cmdline.size()]));
        switch (task.getPipe()) {
        case "stdout":
            builder.redirectError(ProcessBuilder.Redirect.INHERIT);
            break;
        case "stderr":
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            break;
        }

        try {
            Process process = builder.start();

            InputStream stream = null;
            try {
                switch (task.getPipe()) {
                case "stdout":
                    stream = process.getInputStream();
                    break;
                case "stderr":
                    stream = process.getErrorStream();
                    break;
                }

                PluginFileInput input = new PluginFileInput(task, process, stream);
                stream = null;
                return input;

            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        } catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    // TODO almost copied from S3FileInputPlugin. include an InputStreamFileInput utility to embulk-core.
    public static class PluginFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        private static class SingleFileProvider
                implements InputStreamFileInput.Provider
        {
            private Process process;
            private InputStream stream;
            private boolean opened = false;

            public SingleFileProvider(Process process, InputStream stream)
            {
                this.process = process;
                this.stream = stream;
            }

            @Override
            public InputStream openNext() throws IOException
            {
                if (opened) {
                    return null;
                }
                opened = true;
                return stream;
            }

            @Override
            public void close() throws IOException
            {
                if (!opened) {
                    stream.close();
                }
                process.destroy();
                try {
                    process.waitFor();
                } catch (InterruptedException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }

        public PluginFileInput(PluginTask task, Process process, InputStream stream)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(process, stream));
        }

        public void abort() { }

        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }

        @Override
        public void close() { }
    }
}
