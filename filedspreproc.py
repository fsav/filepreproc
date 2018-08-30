from __future__ import print_function

import os, sys
import time
import traceback
import csv
import multiprocessing as multiproc
import pdb

class FileDatasetPreprocessor(object):
    """
    Please see README.md for documentation.

    Args:
        src_dir (str): full path of source directory
        dest_dir (str): full path of destination directory
        preprocess_fn (callable): function that takes as input a source image 
                path (str) and a destination image path (str). See README.md for
                further details.
        input_extension (str): extension of input images. Files that don't have
                this extension will be skipped.
        output_extension (str): extension of output images. Set to None to reuse
                the input format.
        metadata_filename (str): file where to store metadata per file (will
                be created at the root of the destination directory). If left
                to None, no metadata is written.
        metadata_columns (list of str): columns in the metadata file
        num_processes: how many subprocesses to spawn (e.g. you can set this to
                4 if your processor has 4 cores)
    """
    def __init__(self, src_dir, dest_dir, preprocess_fn, input_extension,
                   output_extension=None,
                   metadata_filename=None,
                   metadata_columns=None,
                   num_processes=4):
        save_locals_to_self(self, locals())

        # normalize input/output extensions
        if input_extension[0] != ".":
            self.input_extension = "." + input_extension

        if output_extension is None:
            self.output_extension = self.input_extension

        if self.output_extension[0] != ".":
            self.output_extension = "." + self.output_extension

        # normalize paths, should remove trailing slashes too
        self.src_dir = os.path.abspath(src_dir)
        self.dest_dir = os.path.abspath(dest_dir)

        self.column_order = None
        self.csv_file_path = None

        if self.metadata_filename is not None:
            assert self.metadata_columns is not None

            # copy
            self.metadata_columns = [x for x in self.metadata_columns]

            for k in ['path', 'success', 'message']:
                if k in self.metadata_columns:
                    self.metadata_columns.remove(k)

            self.column_order = ['path','success','message'] + metadata_columns
            print("CSV columns will be: " + str(self.column_order))

            self.csv_file_path = os.path.join(dest_dir, metadata_filename)

    def init_metadata(self):
        pass
        #if not os.path.exists(self.csv_file_path):
            # TODO: write header
            #print("Will process %s" % (src_path,))
            #pass

    def run(self):
        # potentially initialize the CSV file
        self.init_metadata()

        nproc = self.num_processes

        # shared input queue among workers
        queue = multiproc.Queue(maxsize=10)

        # Lock to prevent workers from writing to stdout or CSV simultaneously.
        stdout_lock = multiproc.Lock()
        csv_lock = multiproc.Lock()

        args = (queue, stdout_lock, csv_lock)
        processes = [multiproc.Process(target=self.worker_fn, args=args) \
                        for i in range(nproc)]

        for p in processes:
            p.start()

        cur_src_dir = None
        cur_dest_dir = None
        dir_without_prefix = None

        count = 0
        start_time = time.time()

        for src_path in self.enumerate_files():
            if count > 0 and count % 1000 == 0:
                delta = (time.time() - start_time) / 60.
                file_per_min = count / delta
                _print_with_lock(stdout_lock,
                                 "%d done, total time %f min, file/min %f" % \
                                 (count, delta, file_per_min))

            dir, filename = os.path.split(src_path)

            # When a new directory is entered.
            # (we assume directories are only traversed once but it 
            # shouldn't matter)
            if cur_src_dir != dir:
                cur_src_dir = dir
                dir_without_prefix = dir[len(self.src_dir)+1:]
                cur_dest_dir = os.path.join(self.dest_dir, dir_without_prefix)
                if not os.path.exists(cur_dest_dir):
                    _print_with_lock(stdout_lock,
                                     "Will create dir %s" % (cur_dest_dir,))
                    os.makedirs(cur_dest_dir)

            filebase, ext = os.path.splitext(filename)
            dest_filename = filebase + self.output_extension
            dest_path = os.path.join(cur_dest_dir, dest_filename)

            if os.path.exists(dest_path):
                _print_with_lock(stdout_lock,
                                 "Skipping existing file %s" % (dest_filename,))
                continue
            
            # blocking operation
            queue.put( (src_path, dest_path) )

            # This doesn't count skipped images
            count += 1

        # Stopping mechanism
        # While any process is alive, wait and saturate the queue 
        # with termination signals (None).
        processes_alive = [p for p in processes]
        while len(processes_alive) > 0:
            while not queue.full():
                queue.put(None)
            for p in processes_alive:
                if not p.is_alive():
                    _print_with_lock(stdout_lock,
                                     "Process %d is now dead" % (p.pid,))
                    processes_alive.remove(p)
                    p.join()

        print("Done.")

    def worker_fn(self, input_queue, stdout_lock, csv_lock):
        """Called as the 'target' of multiprocessing.Process.

        Note that obviously this executes in a forked process. Somehow the
        multiprocessing machinery passes a copy of the parent process state
        so that member variables are passed along (e.g. self.preprocess_fn).
        That data is just a copy, though. (See the test at the bottom of
        this file.)
        """
        pid = os.getpid()

        while True:
            next_obj = input_queue.get(block=True)

            # termination signal
            if next_obj is None:
                _print_with_lock(stdout_lock,
                                 "{%d} Got termination signal" % (pid,))
                break

            src_path, dest_path = next_obj

            _print_with_lock(stdout_lock,
                             "{%d} will process %s" % (pid, src_path,))

            success = False
            message = ""
            metadata = {}

            try:
                success, message, metadata = \
                        self.preprocess_fn(src_path, dest_path)
                if not success:
                    # create empty file
                    open(dest_path, "w").close()
                    _print_with_lock(stdout_lock, 
                                     "Reported failure for %s (%s)" % \
                                        (src_path,message,))
            except:
                _print_with_lock(stdout_lock, 
                                 "An error happened for %s" % (src_path,))
                _print_with_lock(stdout_lock, traceback.format_exc())

            # pad with empty values
            for c in self.metadata_columns:
                if c not in metadata:
                    metadata[c] = None

            src_path_without_prefix = src_path[len(self.src_dir)+1:]

            metadata['path'] = src_path_without_prefix
            metadata['success'] = 1 if success else 0
            metadata['message'] = message

            csv_lock.acquire()
            self.write_metadata(metadata)
            csv_lock.release()

    def write_metadata(self, metadata):
        if self.metadata_filename is not None:
            assert metadata is not None
            assert type(metadata) is dict
            # all columns must be present
            assert set(metadata.keys()) == set(self.column_order)

            row = [metadata[k] for k in self.column_order]

            f = open(self.csv_file_path, "a")
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(row)
            f.close()
     
    def enumerate_files(self):
        """Generator that returns all the files under the directory
        """
        extension = self.input_extension
        for src_root, dirs, files in os.walk(self.src_dir):
            assert src_root.startswith(self.src_dir)

            for filename in files:
                _, ext = os.path.splitext(filename)
                if ext.lower() != extension:
                    continue
                yield os.path.join(src_root, filename)

def _print_with_lock(lock, *args):
    lock.acquire()
    print(*args)
    lock.release()

class ForkablePdb(pdb.Pdb):
    """Utility to debug subprocesses launched with multiprocessing.

    Taken from:
    https://stackoverflow.com/questions/4716533/how-to-attach-debugger-to-a-python-subproccess

    To set a breakpoint, simply do : ForkablePdb().set_trace()
    """
    _original_stdin_fd = sys.stdin.fileno()
    _original_stdin = None

    def __init__(self):
        pdb.Pdb.__init__(self, nosigint=True)

    def _cmdloop(self):
        current_stdin = sys.stdin
        try:
            if not self._original_stdin:
                self._original_stdin = os.fdopen(self._original_stdin_fd)
            sys.stdin = self._original_stdin
            self.cmdloop()
        finally:
            sys.stdin = current_stdin

# from pylearn codebase
# useful in __init__(param1, param2, etc.) to save
# values in self.param1, self.param2... just call
# save_locals_to_self(self, locals())
def save_locals_to_self(obj, dct, omit=['self']):
    for k in omit:
        if k in dct:
            del dct[k]
    obj.__dict__.update(dct)


"""
# Test for multiprocessing's ability to pass along class state to the 
# subprocess.
import multiprocessing as multiproc
class MpTest(object):
    def __init__(self):
        self.param1 = "1234"
        self.fn = lambda x: x**2

    def start_procs(self):
        procs = [multiproc.Process(target=self.workerfn) for i in range(3)]
        for i in range(3):
            procs[i].start()

    def workerfn(self):
        print self.param1
        print str(self.fn(12))

if __name__ == '__main__':
    mp = MpTest()
    mp.start_procs()
"""
