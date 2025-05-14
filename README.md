# Distributed Home File System

[Javadocs](https://usatiuk.github.io/dhfs/)

## What is this?

Inspired by Syncthing and Google Drive File Stream, this project
aspires to combine the best of them into one complete solution for
all the file synchronization needs: being decentralized like
Syncthing and allowing you to stream your files like Google Drive File Stream

## Run wrapper

[Download latest build](https://nightly.link/usatiuk/dhfs/workflows/server/main/Run%20wrapper.zip)

This is a simple set of scripts that allows you to run/stop
the DHFS server in the background, and update it.

Once unpacked, in the root folder, there will be 3 folders:

- `app` contains the application
- `data` contains the filesystem data storage
- `fuse` is the default filesystem mount point (not on Windows)

## How to use it?

### General prerequisites

Java should be available as `java` in `PATH`, or with a correctly set `JAVA_HOME`, and Java 21 is required.

FUSE 2 userspace library also should be available:

- On Ubuntu `libfuse2` package can be installed, or an analogous package for other distributions.

- On Windows, [WinFsp](https://winfsp.dev/) should be installed.

- On macOS, [macFUSE](https://macfuse.github.io/).

### How to run it?

In the run-wrapper `app` folder, 3 scripts are available.

- `run` script starts the filesystem
- `stop` script stops it
- `update` script will update the filesystem to the newest available CI build

On Windows, Powershell versions of the scripts should be used. For them to work, it might be required to allow execution of unsigned scripts using `set-executionpolicy unrestricted`.

### Additional options

Additional options for the filesystem can be specified in the `extra-opts` file in the same directory with the run scripts.

One line in the `extra-opts` file corresponds to one option passed to the JVM when starting the filesystem.

Some extra possible configuration options are:

- `-Ddhfs.fuse.root=` specifies the root where filesystem should be mounted. By default, it is the `fuse` path under the `run-wrapper` root. For windows, it should be a disk root, by default `Z:\`.
- `-Ddhfs.objects.last-seen.timeout=` specifies the period of time (in seconds) after which unavailable peers will be ignored for garbage collection and resynchronized after being reconnected. The default is 43200 (30 days), if set to `-1`, this feature is disabled.
- `-Ddhfs.objects.autosync.download-all=` specifies whether all objects (files and their data) should be downloaded to this peer. `true` or `false`, the default is `false`.
- `-Ddhfs.objects.peerdiscovery.port=` port to broadcast on and listen to for LAN peer discovery (default is `42262`)
- `-Ddhfs.objects.peerdiscovery.broadcast=` whether to enable local peer discovery or not (default is `true`)
- `-Dquarkus.http.port=` HTTP port to listen on (default is `8080`)
- `-Dquarkus.http.ssl-port=` HTTPS port to listen on (default is `8443`)
- `-Dquarkus.http.host=` IP address to listen on (default is `0.0.0.0`)
- `-Ddhfs.peerdiscovery.static-peers=` allows to manually specify a peer's address in format of `peer id:http port:https port`, for example `-Ddhfs.peerdiscovery.static-peers=11000000-0000-0000-0000-000000000000:127.0.0.1:9010:9011`

On Windows, the entire space for the filesystem should also be preallocated, the `-Ddhfs.objects.persistence.lmdb.size=` option controls the size (the value is in bytes), on Windows the default is 100 GB.

In case of errors, the standard output is redirected to `quarkus.log` in the `app` folder, on Windows the error output is separate.

### How to connect to other peers?

Then, a web interface will be available at `losthost:8080` (or whatever the HTTP port is), that can be used to connect with other peers. Peers on local network should be available to be connected to automatically.
