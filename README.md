# Command file input plugin for Embulk

This plugin extends a Command and data from its stdout (or stderr).

## Overview

* **Plugin type**: file input
* **Resume supported**: yes (no parallelism)

## Configuration

- **command**: command line (string, required)
- **pipe**: stdout or stderr (string, default: stdout)

The **command** is exected using a shell. So it can include pipe (`|`), environment variables (`$VAR`), redirects, and so on.

## Example

```yaml
in:
  type: command
  command: echo "a,c,c" && echo "1,2,3" && echo "10,11,12" | grep -v 10
```

## Build

```
$ ./gradlew gem
```
