# @author dieutth
# created on 17 Jan 2019

"""
this implementation process pdf files as they are.
"""


import ntpath
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pytesseract import image_to_string
from pdf2image import convert_from_path, convert_from_bytes

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


def write_to_file(output_folder, string):
    filename = ntpath.basename(string[0])
    output_filepath = ntpath.join(output_folder, filename + '.txt')
    text = '\n<===================================================>\n'.join(string[1:])


    with open(output_filepath, 'w') as f:
        f.write(text)


def save_to_disk(ouput_folder, converted):
    converted.foreach(lambda x: write_to_file(ouput_folder, x) )



if __name__ == "__main__":

    conf = SparkConf()
    # conf.set('spark.eventLog.enabled', 'true')
    # conf.set('spark.eventLog.dir', 'file:/tmp/spark-events')
    conf.set('spark.executor.memory', '4G')
    conf.set('spark.driver.memory', '12G')

    sc = SparkContext(conf=conf)

    input_folder = '/Users/dieutth/temp/data/pdf_folder_single/'
    # input_folder = '/Users/dieutth/temp/data/small_input/'

    output_folder = "/Users/dieutth/temp/data/output/"

    start = datetime.now()

    input_files = sc.binaryFiles(input_folder).repartition(12)

    # converted = input_files.map(lambda x: convert_pdf_to_text(x[1]))

    converted = input_files.map(lambda x: convert_pdf_to_text(x[1]))

    # converted.saveAsTextFile(output_folder)
    c = converted.count()
    # input_files = input_files.map(lambda x: x[0])
    # print(input_files.collect())
    # c = input_files.count()
    print(c)
    end = datetime.now()

    print(end-start)
