Embulk::JavaPlugin.register_input(
  "command", "org.embulk.input.CommandFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
