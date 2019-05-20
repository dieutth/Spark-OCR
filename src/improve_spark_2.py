# @author dieutth
# created on 17 Jan 2019

"""
this implementation process pdf files as they are.
"""


import ntpath
import os
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pytesseract import image_to_string
from pdf2image import convert_from_path, convert_from_bytes
from PyPDF2 import PdfFileReader, PdfFileWriter
import tempfile

RESOLUTION = 300  #  default dpi when convert pdf to image


def convert_path_pdf_to_text(input_file):
    """
    Convert pdf file to text, where input is the file location.
    This is expensive as it needs to read file from storage.

    :param input_file: absolute path to pdf file
    :return:
    """
    input_file = input_file[5:]  #  remove prefix file:/ in filename
    images = convert_from_path(input_file, RESOLUTION)
    pages = []
    pages.append(input_file)

    for image in images:
        page = image_to_string(image)
        pages.append(page)
    return pages


def convert_pdf_to_text(pdf_file):
    """
    Convert pdf byte stream to text.

    :param pdf_file: content of pdf file as a byte stream
    :return:
    """
    images = convert_from_bytes(pdf_file, RESOLUTION)

    pages = [image_to_string(image) for image in images]

    return pages


def split_pdf(pdf_file):
    pages = PdfFileReader(pdf_file)
    nb_pages = pages.getNumPages()
    pages.getPage()


def write_to_file(output_folder, string):
    filename = ntpath.basename(string[0])
    output_filepath = ntpath.join(output_folder, filename + '.txt')
    text = '\n<===================================================>\n'.join(string[1:])


    with open(output_filepath, 'w') as f:
        f.write(text)


def save_to_disk(ouput_folder, converted):
    converted.foreach(lambda x: write_to_file(ouput_folder, x) )




def cut_pdf(pdf):
    reader = PdfFileReader(pdf)
    nb_pages = reader.getNumPages()
    files = []
    for page_nb in range(nb_pages):
        writer = PdfFileWriter()
        writer.addPage(reader.getPage(page_nb))
        fh, temp_filename = tempfile.mkstemp(dir='Users/dieutth/temp/temp/')
        try:
            with open(temp_filename, 'wb') as f:
                writer.write(f)
                f.flush()

        finally:
            file.append(temp_filename)
            os.close(fh)
            # os.remove(temp_filename)
    return files




if __name__ == "__main__":

    conf = SparkConf()
    conf.set('spark.eventLog.enabled', 'true')
    conf.set('spark.eventLog.dir', 'file:/tmp/spark-events')
    conf.set('spark.executor.memory', '4G')
    conf.set('spark.driver.memory', '12G')
    sc = SparkContext(conf=conf)

    input_folder = '/Users/dieutth/temp/data/pdf_folder_single/'

    output_folder = "/Users/dieutth/temp/data/output/"

    start = datetime.now()


    # print (image_to_string(convert_from_path(input_folder + 'tpbank.pdf')[0]))

    input_files = sc.binaryFiles(input_folder).repartition(12)

    splitted_pdf = input_files.flatMap(lambda pdf: cut_pdf(pdf[0][5:]))

    splitted = splitted_pdf.flatMapValues(lambda x: convert_from_bytes(x, RESOLUTION))

    converted = splitted.mapValues(lambda x: image_to_string(x))

    # converted.saveAsTextFile(output_folder)
    c = converted.count()
    # input_files = input_files.map(lambda x: x[0])
    # print(input_files.collect())
    # c = input_files.count()
    print(c)
    end = datetime.now()

    print(end-start)
