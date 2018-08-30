# filepreproc

This Python module is file-based dataset preprocessor, using Python "multiprocessing" to provide true parallelism (ie. circumvent the GIL). It will traverse the source directory, apply a user-supplied procedure (Python function) to each file it encounters, and save the result in a mirror directory structure.  It also produces a CSV file with one row per file processed, based on metadata returned by the function.

It should work both in Python 2.7 and Python 3.x.

I call it "preprocessor" because *my use case* is to use it to preprocess a dataset of images (for ML model training), apply a preprocessing on them and record a set of attributes for each image along the way.

Files already present in the destination directory will be skipped, thus allowing the operation to be stopped and resumed. *Note that interrupting the process with Ctrl+C might lead to a file being written without the corresponding line written to the CSV file*. Also note you may need to press Ctrl+C multiple times as there are multiple processes.

## Usage example

See `usage_example.py` for a dummy preprocessing example. It converts images to grayscale. The output CSV will contain the image width and height as columns (along with the 3 default columns).

## Warning

* I've tested this code by preprocessing fairly large datasets with it (e.g. IMDB-WIKI, ~500k images), but it *still requires (ideally) unit tests and (at least) some system tests*. Ie. use with care.
    * Also I recommend you *chmod to readonly* your input dataset in case of typos in the parameters you supply.

## Class documentation

What follows serves as class documentation, to avoid cluttering the code with my flowery prose.

### The user-supplied procedure

The user-supplied "function", `preprocess_fn`, is expected to read the file and output another corresponding file in an output directory with mirrored structure. As such it isn't a pure function (it has side-effects) so we may also call it a "procedure".

`preprocess_fn` must take two inputs:
* The first is the *source path* from which to read the file
* The first is the *destination path* to which it should write the corresponding preprocessed file

Empty files are created by FileDatasetPreprocessor itself when the output file doesn't exist after the call to `preprocess_fn`, or if an exception is caught. This is to ensure erroneous files aren't processed twice if the operation is stopped and resumed.

`preprocess_fn` must return three elements:
* `is_success`: boolean indicating True for success, meaning the result is usable, False if some error occurred or if the file was rejected
* `message`: optional string that will be written to the CSV file, e.g. rejection reason
* `metadata`: dictionary of metadata written as columns in the CSV file. This can be left to None if constructor parameter `metadata_filename` is None. 

### Outputted CSV file

The FileDatasetPreprocessor can optionally store a CSV with custom columns. If needed, `preprocess_fn` must return a dictionary with the metadata. Expected columns are defined by the `metadata_columns` parameter.

Three additional columns will be prepended to the user-defined ones:
* `filepath`
* `success` (bool, 0 or 1): corresponds to `is_success` as returned by `preprocess_fn`
* `message`: the same `message` returned by `preprocess_fn`

No need to specify those. No data type checking is performed. The CSV file is written row by row (no buffering) to avoid inconsistent results if the program is interrupted.

FileDatasetPreprocessor does not delete or read the existing metadata file. It simply appends to it. You're responsible for deleting it if you restart the preprocessing from scratch.

A corollary is that the CSV file will not contain a first row with the column names. Instead the order is printed on stdout when the program starts (this could use improvement).

## Other stuff

There's a small class in there called ForkablePdb which helps in pdb-debugging the subprocesses. (It's taken from StackOverflow.)

Performance note: apparently multiprocessing.Queue.get is pretty slow, but here I'm assuming the overhead is not important relative to the rather heavy processing done for each file.

https://stackoverflow.com/questions/8463008/python-multiprocessing-pipe-vs-queue
https://stackoverflow.com/questions/48254216/multiprocessing-producer-consumer-with-python-3-x-on-linux

