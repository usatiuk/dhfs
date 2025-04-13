package com.usatiuk.dhfs.files.service;

public record GetattrRes(long mtime, long ctime, long mode, GetattrType type) {
}
