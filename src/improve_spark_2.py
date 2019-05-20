# @author dieutth
# created on 17 Jan 2019

"""
this implementation process pdf files as they are.
"""


import ntpath
import os
import re
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


def map_file_name(input_file_name_content):
    file_name = re.split("/", input_file_name_content[0])[-1]
    split_result = re.split("_", file_name)
    name = split_result[0]
    part = split_result[1]
    part_number = int(part[:-4])
    content = input_file_name_content[1]
    return (name, (part_number, content))


def write_to_file(output_folder, filename, content):
    output_filepath = ntpath.join(output_folder, filename + '.txt')
    with open(output_filepath, 'w') as f:
        f.write(content)


def save_to_disk(ouput_folder, converted):
    converted.foreach(lambda x: write_to_file(ouput_folder, x[0], x[1]) )


def merge_lists(d1, d2):
    for item in d2:
        d1.append(item)
    return d1

def merge_string(list_of_tuple):
    return '\n'.join('\n'.join(item[1]) for item in list_of_tuple)


if __name__ == "__main__":

    conf = SparkConf()
    conf.set('spark.eventLog.enabled', 'true')
    conf.set('spark.eventLog.dir', 'file:/tmp/spark-events')
    conf.set('spark.executor.memory', '4G')
    conf.set('spark.driver.memory', '12G')
    sc = SparkContext(conf=conf)

    input_folder = '/Users/xizhang/workspace/experimentations/Spark-OCR/resources/input/'

    output_folder = "/Users/xizhang/workspace/experimentations/Spark-OCR/output-2/"

    start = datetime.now()


    # print (image_to_string(convert_from_path(input_folder + 'tpbank.pdf')[0]))

    input_files = sc.binaryFiles(input_folder).repartition(12)

    # splitted_pdf = input_files.flatMap(lambda pdf: cut_pdf(pdf[0][5:]))
    #
    # splitted = splitted_pdf.flatMapValues(lambda x: convert_from_bytes(x, RESOLUTION))

    mapped_rdd = input_files.map(lambda x: map_file_name(x))
    # print(converted)

    converted = mapped_rdd.mapValues(lambda ele: [(ele[0], convert_pdf_to_text(ele[1]))] )


    merged_list = converted.reduceByKey(merge_lists)

    sorted_list = merged_list.mapValues(lambda l: sorted(l, key=lambda tup: tup[0]))

    merged_string = sorted_list.mapValues(lambda l: merge_string(l))


    save_to_disk(output_folder, merged_string)
    # write_to_file(output_folder, merged_string[0], merged_string.[1])

    end = datetime.now()

    print(end-start)
