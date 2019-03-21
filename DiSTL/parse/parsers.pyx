"""
cython methods to speed up count generation
"""
cimport cython

# TODO handle bigram term map

cdef dict counter(list tokens):
    """generates the counts for all terms in tokens"""

    cdef dict term_dict = {}

    for t in tokens:
        try:
            term_dict[t] += 1
        except:
            term_dict[t] = 1

    return term_dict


cdef dict counter_map(list tokens, dict term_map):
    """generates counts constrained to a term map"""

    cdef dict term_dict = {}

    for t in tokens:
        try:
            t = term_map[t]
        except:
            pass

        try:
            term_dict[t] += 1
        except:
            term_dict[t] = 1

    return term_dict


def counter_update(list tokens, dict term_dict):
    """updates an existing term_dict with new vals"""

    cdef dict n_term_dict = counter(tokens)

    for t in n_term_dict:
        try:
            term_dict["doc_count"][t] += 1
            term_dict["term_count"][t] += n_term_dict[t]
        except:
            term_dict["doc_count"][t] = 1
            term_dict["term_count"][t] = n_term_dict[t]

    return term_dict


def counter_map_update(list tokens, dict term_dict, dict term_map):
    """updates an existing term_dict with a term_map"""

    cdef dict n_term_dict = counter_map(tokens, term_map)

    for t in n_term_dict:
        try:
            term_dict["doc_count"][t] += 1
            term_dict["term_count"][t] += n_term_dict[t]
        except:
            term_dict["doc_count"][t] = 1
            term_dict["term_count"][t] = n_term_dict[t]

    return term_dict
