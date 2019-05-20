# @author dieutth
# created on 17 Jan 2019

"""
this implementation process pdf files as they are.
"""


from os import path, listdir
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pytesseract import image_to_string
import ntpath

from src.__init__ import INPUT_PATH, OUTPUT_PATH, TEMPLATE_PATH
from src.utils.pdf_to_image import use_pdf2image_lib

# Globals
RESOLUTION = 300  #  default dpi when convert pdf to image
SIZE = 256
REPARTITION = 12

def get_images(pdf_file, n):

    base = path.basename(pdf_file)
    filename = path.splitext(base)[0]
    images = use_pdf2image_lib(pdf_file)
    return [(filename, images[i:i + n], i) for i in range(0, len(images), n)]


def convert_images_to_text(images):
    """
    Convert pdf byte stream to text.

    :param pdf_file: content of pdf file as a byte stream
    :return:
    """
    pages = [image_to_string(image) for image in images[1]]

    return (images[0], pages, images[2])

def merge_string(list_of_tuple):
    return '\n'.join(item[0] for item in list_of_tuple)

def write_to_file(output_folder, filename, content):
    output_filepath = path.join(output_folder, filename + '.txt')
    with open(output_filepath, 'w') as f:
        f.write(content)

def save_to_disk(ouput_folder, converted):
    converted.foreach(lambda x: write_to_file(ouput_folder, x[0], x[1]) )

if __name__ == "__main__":

    conf = SparkConf()
    conf.set('spark.eventLog.enabled', 'true')
    conf.set('spark.executor.memory', '1G')
    conf.set('spark.driver.memory', '1G')
    sc = SparkContext(conf=conf)

    start = datetime.now()

    input_files = [
        path.join(INPUT_PATH, f)
        for f in listdir(INPUT_PATH) if f.endswith('.pdf')
    ]

    init_rdd = sc.parallelize([get_images(pdf_file, 1) for pdf_file in input_files])

    rdd_splitted = init_rdd.flatMap(lambda col: col)

    converted = rdd_splitted.map(lambda ele: convert_images_to_text(ele))

    sorted_list = converted.sortBy(lambda x: (x[0],x[2]))

    merged_list = (
        sorted_list
        .map(lambda nameTuple: (nameTuple[0], [ nameTuple[1] ]))
        .reduceByKey(lambda a, b: a + b)
    )

    # merged_list.foreach(print)

    merged_string = merged_list.mapValues(lambda l: merge_string(l))

    save_to_disk(OUTPUT_PATH, merged_string)


    end = datetime.now()
    print(end - start)
