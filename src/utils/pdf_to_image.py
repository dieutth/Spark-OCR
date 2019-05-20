from pdf2image import convert_from_path
from wand.image import Image

def use_pdf2image_lib(input_path):
    image = convert_from_path(input_path, 300)
    return image


def use_wand_lib(input_path):
    image = Image(filename=input_path, resolution=300)
    return image