import os
from datetime import datetime
from src.utils.pdf_to_image import use_pdf2image_lib, use_wand_lib

import argparse

"""
Comparing the 2 popular python libraries that convert pdf file to images: pdf2image and wand
Use 2 libs to convert pdf to image with same default resolution = 300.
"""


def run_benchmark(input_files):
    start = datetime.now()
    for file in input_files:
        use_pdf2image_lib(file)
    end = datetime.now()
    print('use pdf2image performance: ' + str(end-start))


    start = datetime.now()
    for file in input_files:
        use_wand_lib(file)
    end = datetime.now()
    print('use wand performance: ' + str(end-start))


if __name__ is not '__main__':

    parser = argparse.ArgumentParser(description='Creates the O-tables in Vortex Processing')
    parser.add_argument('-i', dest='input_folder', action='store',
                        default='/Users/dieutth/temp/data/pdf_folder_single',
                        help='input folder that contains pdf files')

    arguments = parser.parse_args()

    input_folder = arguments.input_folder

    input_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder)if f.endswith('.pdf')]

    run_benchmark(input_files)

    """
    Running this benchmark over a folder of 114 pdf file, each file contains 1-3 pages, 
    use pdf2image performance: 0:01:23.913140
    use wand performance: 0:03:31.981430
    """