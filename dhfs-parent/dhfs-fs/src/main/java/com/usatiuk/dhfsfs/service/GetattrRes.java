package com.usatiuk.dhfsfs.service;

/**
 * GetattrRes is a record that represents the result of a getattr operation.
 * @param mtime File modification time
 * @param ctime File creation time
 * @param mode File mode
 * @param type File type
 */
 public record GetattrRes(long mtime, long ctime, long mode, GetattrType type) {
}
