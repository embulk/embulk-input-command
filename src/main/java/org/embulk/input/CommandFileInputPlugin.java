package org.embulk.input;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.file.InputStreamFileInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandFileInputPlugin
        implements FileInputPlugin {
    public interface PluginTask
            extends Task {
        @Config("command")
        public String getCommand();

        @Config("pipe")
        @ConfigDefault("\"stdout\"")
        public String getPipe();

    }

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        final PluginTask task = configMapper.map(config, PluginTask.class);

        switch (task.getPipe()) {
            case "stdout":
                break;
            case "stderr":
                break;
            default:
                throw new ConfigException(String.format(
                        "Unknown 'pipe' option '%s'. It must be either 'stdout' or 'stderr'", task.getPipe()));
        }

        return resume(task.toTaskSource(), 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control) {
        control.run(taskSource, taskCount);

        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        List<String> cmdline = new ArrayList<String>();
        cmdline.addAll(buildShell());
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
            default:
                throw new IllegalStateException(String.format(
                        "Unknown 'pipe' option '%s'. It must be either 'stdout' or 'stderr'", task.getPipe()));
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
                    default:
                        throw new IllegalStateException(String.format(
                                "Unknown 'pipe' option '%s'. It must be either 'stdout' or 'stderr'", task.getPipe()));
                }

                PluginFileInput input = new PluginFileInput(task, new ProcessWaitInputStream(stream, process));
                stream = null;
                return input;

            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    static List<String> buildShell() {
        String osName = System.getProperty("os.name");
        if (osName.indexOf("Windows") >= 0) {
            return Collections.unmodifiableList(Arrays.asList("PowerShell.exe", "-Command"));
        } else {
            return Collections.unmodifiableList(Arrays.asList("sh", "-c"));
        }
    }

    private static class ProcessWaitInputStream
            extends FilterInputStream {
        private Process process;

        public ProcessWaitInputStream(InputStream in, Process process) {
            super(in);
            this.process = process;
        }

        @Override
        public int read() throws IOException {
            int c = super.read();
            if (c < 0) {
                waitFor();
            }
            return c;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int c = super.read(b);
            if (c < 0) {
                waitFor();
            }
            return c;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int c = super.read(b, off, len);
            if (c < 0) {
                waitFor();
            }
            return c;
        }

        @Override
        public void close() throws IOException {
            super.close();
            waitFor();
        }

        private synchronized void waitFor() throws IOException {
            if (process != null) {
                int code;
                try {
                    code = process.waitFor();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                process = null;
                if (code != 0) {
                    throw new IOException(String.format(
                            "Command finished with non-zero exit code. Exit code is %d.", code));
                }
            }
        }
    }

    // TODO almost copied from S3FileInputPlugin. include an InputStreamFileInput utility to embulk-core.
    public static class PluginFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput {
        private static class SingleFileProvider
                implements InputStreamFileInput.Provider {
            private final InputStream stream;
            private boolean opened = false;

            public SingleFileProvider(InputStream stream) {
                this.stream = stream;
            }

            @Override
            public InputStream openNext() throws IOException {
                if (opened) {
                    return null;
                }
                opened = true;
                return stream;
            }

            @Override
            public void close() throws IOException {
                if (!opened) {
                    stream.close();
                }
            }
        }

        public PluginFileInput(PluginTask task, InputStream stream) {
            super(Exec.getBufferAllocator(), new SingleFileProvider(stream));
        }

        public void abort() {
        }

        public TaskReport commit() {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        @Override
        public void close() {
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(CommandFileInputPlugin.class);

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
}
