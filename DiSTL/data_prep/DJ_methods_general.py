from io import StringIO
import xml.etree.ElementTree as ET
import re


def doc_generator(f):
    """takes a file containing XML data from DJN and returns a generator
    that yields documents

    Parameters
    ----------
    f : str
        file location containing XML documents

    Returns
    -------
    generator yielding documents
    """

    remove_re0 = re.compile(u"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]")
    remove_re1 = re.compile("&#.+?;")

    doctype_str = '<!DOCTYPE doc SYSTEM "djnml-1.0b.dtd">\n'
    iso0_str = '<?xml version="1.0" encoding="ISO-8859-1" ?>\n'
    iso1_str = '<?xml version="1.0" encoding="iso-8859-1" ?>\n'
    utf8_str = '<?xml version="1.0" encoding="UTF-8" ?>\n'

    try:
        with open(f, "r", encoding="utf-8") as ifile:
            data = ifile.read()
    except:
        try:
            with open(f, "r", encoding="latin1") as ifile:
                data = ifile.read()
        except:
            with open(f, "r", encoding="ascii") as ifile:
                data = ifile.read()

    data = data.replace("%s%s" % (iso0_str, doctype_str), "")
    data = data.replace("%s%s" % (iso1_str, doctype_str), "")
    data = data.replace("%s%s" % (utf8_str, doctype_str), "")
    data = "<root>" + data + "</root>"
    data = iso0_str + doctype_str + data
    data = remove_re0.subn("", data)[0]
    data = remove_re1.subn("", data)[0]
    data = ET.parse(StringIO(data))
    iterator = data.getiterator("doc")

    return iterator
