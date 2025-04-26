# Distributed Home File System 🚧

## What is this?

Inspired by Syncthing and Google Drive File Stream, this project
aspires to combine the best of them into one complete solution for
all the file synchronization needs: being decentralized like
Syncthing and allowing you to stream your files like Google Drive File Stream

## Run wrapper

[Download latest build](https://nightly.link/usatiuk/dhfs/workflows/server/main/Run%20wrapper.zip)

This is a simple wrapper around the jar/web ui distribution that allows you to run/stop
the DHFS server in the background, and update itself (hopefully!)

## How to use it?


Unpack the run-wrapper and run the `run` script. The filesystem should be mounted to the `fuse` folder in the run-wrapper root directory.

Then, a web interface will be available at `losthost:8080`, that can be used to connect with other peers.
