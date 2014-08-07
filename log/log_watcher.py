#!/usr/bin/env python

"""
Real-time log file watcher supporting log rotation.
Original Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
Modified by Maslino@github
License: MIT
"""

import os
import time
import stat


class LogWatcher(object):
    """Looks for changes in a log file.
    This is useful for watching log file changes in real-time.
    It also supports file rotation.
    """

    def __init__(self, log_path, callback, size_hint=1048576):
        """Arguments:

        (str) @log_path:
            the log file to watch

        (callable) @callback:
            a function which is called every time one of the file being
            watched is updated;
            this is called with "filename" and "lines" arguments.

        (int) @size_hint: passed to file.readlines(), represents an
            approximation of the maximum number of bytes to read from
            a file on every iteration (as opposed to load the entire
            file in memory until EOF is reached). Defaults to 1MB.
        """
        self._log_path = os.path.realpath(log_path)
        self._file_pair = [None, None]  # [file_id, file_obj]
        self._callback = callback
        self._size_hint = size_hint

        assert os.path.isfile(self._log_path), self._log_path
        assert callable(callback), repr(callback)

        self.update_file()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def loop(self, interval=0.1, blocking=True):
        """Start a busy loop checking for file changes every *interval*
        seconds. If *blocking* is False make one loop then return.
        """
        while True:
            self.update_file()
            self.readlines(self._file_pair[1])
            if not blocking:
                return
            time.sleep(interval)

    @staticmethod
    def log(line):
        """Log when a file is un/watched"""
        print(line)

    @classmethod
    def open(cls, filename):
        """Wrapper around open().
        """
        return open(filename, 'r')

    def update_file(self):
        # only rotation is considered here
        try:
            st = os.stat(self._log_path)
        except EnvironmentError as e:
            self.log(str(e))
            # rotation procedure not finished yet
            return

        assert stat.S_ISREG(st.st_mode)

        new_file_id = self.get_file_id(st)
        if self._file_pair[0] is None:
            self.watch(self._log_path)
        elif self._file_pair[0] != new_file_id:
            # log file has been rotated. reload it.
            self.unwatch(self._file_pair[1])
            self.watch(self._log_path)

    def readlines(self, file_obj):
        """Read file lines since last access until EOF is reached and
        invoke callback.
        """
        while True:
            lines = file_obj.readlines(self._size_hint)
            if not lines:
                break

            last_line = lines[-1]
            # incomplete line
            while last_line[-1] != '\n':
                next_line = file_obj.readline()
                last_line = last_line + next_line
            lines[-1] = last_line

            self._callback(file_obj.name, lines)

    def watch(self, filename):
        file_obj = self.open(filename)
        file_id = self.get_file_id(os.stat(filename))
        self.log("watching logfile %s" % filename)
        self._file_pair = [file_id, file_obj]

    def unwatch(self, file_obj):
        # File no longer exists. If it has been renamed try to read it
        # for the last time in case we're dealing with a rotating log
        # file.
        self.log("un-watching logfile %s" % file_obj.name)
        self._file_pair = [None, None]
        with file_obj:
            self.readlines(file_obj)

    @staticmethod
    def get_file_id(st):
        return "%x-%x" % (st.st_dev, st.st_ino)

    def close(self):
        file_obj = self._file_pair[1]
        if file_obj is not None:
            file_obj.close()


if __name__ == "__main__":
    def callback(filename, lines):
        for line in lines:
            print filename, line

    lw = LogWatcher("/tmp/log/test.log", callback)
    lw.loop()
