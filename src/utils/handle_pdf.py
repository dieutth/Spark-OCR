# @author dieutth
# created on 09 April 2019

# This scripts utilize PDFFileReader and PDFFileWriter to split and merge pdf files

import ntpath
from PyPDF2 import PdfFileReader, PdfFileWriter

def split_pdf(pdf_file, nb_page_per_doc=1, output_folder=None):
    """
    Split a pdf file to multiple files.

    :param pdf_file absolute path to the pdf file
    :param nb_page_per_doc number of pages for the smaller file, default 1
    :param ouput_folder location to store the splitted files
    """
    pages = PdfFileReader(pdf_file)

    # if output_folder is not provided (None), store splitted files to the same folder as the input file
    if output_folder is None:
        output_folder = ntpath.dirname(pdf_file)

    filename = ntpath.basename(pdf_file)
    filename_without_extension = filename[:-4]

    loops = int(pages.getNumPages()/nb_page_per_doc)

    for loop in range(loops):

        output = PdfFileWriter()

        start = loop * nb_page_per_doc
        end = (loop+1) * nb_page_per_doc

        for page_nb in range(start, end):
            output.addPage(pages.getPage(page_nb))

        path = ntpath.join(output_folder, filename_without_extension + '_' + str(loop) + '.pdf')

        with open(path, 'wb') as outfile:
            output.write(outfile)


    output = PdfFileWriter()
    for page_nb in range(nb_page_per_doc * loops, pages.getNumPages()):
        output.addPage(pages.getPage(page_nb))

    path = ntpath.join(output_folder, filename_without_extension + '_' + str(loop+1) + '.pdf')

    with open(path, 'wb') as outfile:
        output.write(outfile)



def merge_pdfs(input_files, output_path):
    """
     Merge multiple pdf files into one and save the output to disk

    :param input_files a list of file path (string)
    :param output_path path to save the merged file

    """
    output = PdfFileWriter()
    for file in input_files:
        pages = PdfFileReader(file)
        for i in range(0, pages.getNumPages()):
            output.addPage(pages.getPage(i))

    with open(output_path, 'wb') as outfile:
        output.write(outfile)
