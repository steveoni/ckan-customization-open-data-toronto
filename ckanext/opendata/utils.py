from ckan.lib.navl.dictization_functions import missing

import ckan.plugins.toolkit as tk

import codecs


def string_to_hex(s):
    return codecs.encode(s.encode('utf-8'), 'hex')

def hex_to_string(s):
    return codecs.decode(s, 'hex').decode('utf-8')

def is_geospatial(resource_id):
    info = tk.get_action('datastore_info')(None, { 'id': resource_id })

    return 'geometry' in info['schema']

def to_list(l):
    if not isinstance(l, list):
        return [l]

    return l

def validate_length(key, data, errors, context):
    if data[key] and len(data[key]) > constants.MAX_FIELD_LENGTH:
        raise tk.ValidationError({
            'constraints': [
                'Input exceed {0} character limit'.format(
                    constants.MAX_FIELD_LENGTH
                )
            ]
        })

    return data[key]

def validate_tag_in_vocab(tag, vocab):
    try:
        tk.get_action('tag_show')(None, { 'id': tag, 'vocabulary_id': vocab })
    except:
        raise tk.ValidationError({
            'constraints': [
                'Tag {0} is not in the vocabulary {1}'.format(tag, vocab)
            ]
        })
