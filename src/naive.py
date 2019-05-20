# @author dieutth
# created on 17 Jan 2019

"""
this implementation process pdf files as they are.
"""


import os.path
from datetime import datetime
from pytesseract import image_to_string

from src.utils.pdf_to_image import use_pdf2image_lib
from src.__init__ import INPUT_PATH, OUTPUT_PATH

# Goals :
# improve Naive by removing intermediary disk IO:
# Read PDFs => convert to images (in memory)
#  => feed images to tesseract (in memory)
#  to get text => save text to [D]FS

# Globals
RESOLUTION = 300


if __name__ == "__main__":

    start = datetime.now()

    input_files = [
        os.path.join(INPUT_PATH, f)
        for f in os.listdir(INPUT_PATH) if f.endswith('.pdf')
    ]

    for pdf_file in input_files:
        base = os.path.basename(pdf_file)
        filename = os.path.splitext(base)[0]

        images = use_pdf2image_lib(pdf_file)

        for image in images :
            text_file = image_to_string(image)
            page_number = images.index(image)
            output_filename = (
                "{output_path}/{filename}_page{page_number}.txt".format(
                    output_path=OUTPUT_PATH,
                    filename=filename,
                    page_number=page_number,
                )
            )
            print(output_filename)
            with open(output_filename, "w") as file_output:
                file_output.write(text_file)

    end = datetime.now()

    print(end-start)
