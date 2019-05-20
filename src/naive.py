#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
this implementation process pdf files as they are.
"""


from os import path, listdir
from datetime import datetime
from pytesseract import image_to_string

from src.utils.pdf_generator import generate_pdfs
from src.utils.pdf_to_image import use_pdf2image_lib
from src.__init__ import INPUT_PATH, OUTPUT_PATH, TEMPLATE_PATH

# Goals :
# improve Naive by removing intermediary disk IO:
# Read PDFs => convert to images (in memory)
#  => feed images to tesseract (in memory)
#  to get text => save text to [D]FS

# Globals
RESOLUTION = 300
TEMPLATE_NAME = [f for f in listdir(TEMPLATE_PATH) if f.endswith('.pdf')][0]
TEMPLATE_FILE = path.join(TEMPLATE_PATH, TEMPLATE_NAME)
PDF_FILES_NUMBER = 3
PDF_PAGES_NUMBER = 3


if __name__ == "__main__":

    generate_pdfs(TEMPLATE_FILE, PDF_FILES_NUMBER, PDF_PAGES_NUMBER, INPUT_PATH)

    start = datetime.now()

    input_files = [
        path.join(INPUT_PATH, f)
        for f in listdir(INPUT_PATH) if f.endswith('.pdf')
    ]

    for pdf_file in input_files:
        base = path.basename(pdf_file)
        filename = path.splitext(base)[0]

        images = use_pdf2image_lib(pdf_file)

        output_filename = "{output_path}/{filename}.txt".format(
            output_path=OUTPUT_PATH,
            filename=filename,
        )

        with open(output_filename, "w") as file_output:
            for image in images:
                text_file = image_to_string(image)
                file_output.write(text_file)

    end = datetime.now()

    print(end - start)
