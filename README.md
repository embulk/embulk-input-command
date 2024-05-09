# embulk-input-command

Command file input plugin for Embulk: runs a command and reads data from its stdout (or stderr).

## Overview

* **Plugin type**: file input
* **Resume supported**: yes (no parallelism)

## Configuration

- **command**: command line (string, required)
- **pipe**: stdout or stderr (string, default: stdout)

The **command** is exected using a shell (`sh -c` on UNIX/Linux, `PowerShell.exe -Command` on Windows). Therefore, it can include pipe (`|`), environment variables (`$VAR`), redirects, and so on.

## Example

```yaml
in:
  type: command
  command: echo "a,c,c" && echo "1,2,3" && echo "10,11,12" | grep -v 10
```

For Maintainers
----------------

### Release

Modify `version` in `build.gradle` at a detached commit, and then tag the commit with an annotation.

```
git checkout --detach master

(Edit: Remove "-SNAPSHOT" in "version" in build.gradle.)

git add build.gradle

git commit -m "Release vX.Y.Z"

git tag -a vX.Y.Z

(Edit: Write a tag annotation in the changelog format.)
```

See [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) for the changelog format. We adopt a part of it for Git's tag annotation like below.

```
## [X.Y.Z] - YYYY-MM-DD

### Added
- Added a feature.

### Changed
- Changed something.

### Fixed
- Fixed a bug.
```

Push the annotated tag, then. It triggers a release operation on GitHub Actions after approval.

```
git push -u origin vX.Y.Z
```
