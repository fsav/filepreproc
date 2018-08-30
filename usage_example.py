# This is a dummy preprocessing example to show how to use 
# FileDatasetPreprocessor. It converts images to grayscale. 
# The output CSV will contain the image width and height as columns 
# (along with the 3 default columns).

import os
from PIL import Image as pilimg

from filedspreproc import FileDatasetPreprocessor

def test_preprocessor(src_img_path, dest_img_path):
    image = pilimg.open(src_img_path)
    width = image.size[0]
    height = image.size[1]
    # just some dummy processing: convert to grayscale
    image.convert('L').save(dest_img_path)
    return True, "", {"width": width, "height": height}

if __name__ == '__main__':
    # "testimgs" simply contains jpg images in various subdirectories
    # "testimgs_out" is initially empty
    src_dir = os.path.expanduser("~/data/testimgs")
    dest_dir = os.path.expanduser("~/data/testimgs_out")
    proc = FileDatasetPreprocessor(src_dir=src_dir,
                                   dest_dir=dest_dir,
                                   preprocess_fn=test_preprocessor,
                                   input_extension="jpg",
                                   metadata_filename="preprocessed.csv",
                                   metadata_columns=["width", "height"],
                                   num_processes=4)
    proc.run()
