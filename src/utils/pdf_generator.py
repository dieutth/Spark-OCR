import sys
import random
from PyPDF2 import PdfFileWriter, PdfFileReader


def generate_pdfs(base_pdf_path: str, n_pdfs: int, n_pages: int, output_folder: str) -> None:
    """
    Generate pdf files by randomly selecting pages from a given base pdf file.

    :param base_pdf_path: The path to the pdf file from which more files will be generated.
    :param n_pdfs: The number of pdf files to be generated.
    :param n_pages: The number of pages each pdf file has.
    :param output_folder: The path to the folder where generated files locate.
    :return: None
    """

    base_pdf = PdfFileReader(open(base_pdf_path, "rb"))
    n_pages_base_pdf = base_pdf.numPages

    for counter in range(n_pdfs):
        # randomly select pages from the input file
        page_numbers = random.sample(range(n_pages_base_pdf), n_pages)
        # add the randomly selected pages
        output = PdfFileWriter()
        for i in page_numbers:
            output.addPage(base_pdf.getPage(i))
        # save this pdf to disk
        output_pdf_name = output_folder + "/" + str(counter) + ".pdf"
        with open(output_pdf_name, "wb") as f:
            output.write(f)


if __name__ == "__main__":

    root_file_name = sys.argv[1]
    output_path = sys.argv[2]
    n_pdf_to_be_generated = 10
    n_page_within_pdf = 2
    generate_pdfs(root_file_name, n_pdf_to_be_generated, n_page_within_pdf, output_path)
