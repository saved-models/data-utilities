from collections.abc           import Iterable
from linkml.utils.schemaloader import SchemaLoader
from linkml.validator          import validate_file
import logging
from os.path import isfile
from pathlib import PurePath

def fst(g):
    """
    Utility function. RDFLib gives generators all over the place and
    usually we just want a single value.
    """
    for e in g:
        return e
    raise Exception("Generator is empty")

def validation_helper (data         : str
                     , schema       : str
                     , target_class : str) -> bool:
    '''
    `validate_file()' either returns an empty list or a collection of
    errors in a report (`linkml.validator.report.ValidationReport').
    
    Setting the `strict' flag means that it fails on the first error,
    so we only get one. I think this behaviour is better as it catches
    the first error and should make it easier to fix.

    Compared to the hideous Python Traceback, these errors are remarkably
    friendly and informative!
    '''
    logging.debug (f"Called `validate_wrapper (data = {data}, schema = {schema}, target_class = {target_class})'")
    prereq_check = isfile (data) and isfile (schema)

    if (prereq_check):
        report  = validate_file (data, schema, target_class, strict = True)
        results = report.results

        if (not results):
            logging.info (f"Validation success: data file {data} against schema file {schema}, with target class {target_class}")
            return (True)
        else:
            single_result = results[0]
            severity = single_result.severity
            problem  = single_result.message
            instance = single_result.instance
            
            print ("Validation error: ")
            print (f"-> Severity: {severity}")
            print (f"-> Message: {problem}")
            print (f"-> Trace: {instance}")
        
            return (False)
    else:
        print (f"Data file {data} and schema file {schema} must exist!")
        return (prereq_check)

def extension_helper (target_path : PurePath) -> str:
    '''
    Get the extension without the leading dot,
    to feed into `get_loader', `get_dumper' &c.
    '''
    target = str (target_path)
    if (len (target) == 0):
        return (target)
    else:
        return (target_path.suffix [1 : len (target_path.suffix)])

def schema_components_helper (schema : str) -> dict [str, str]:
    """
    A shim which serialises the schema proper, to extract components of
    interest, so that they can be serialised in the manifest `tables'
    section.
    """
    logging.debug (f"Calling `conversion_shim (schema = {schema})'")
    schema_obj = SchemaLoader (schema).schema
    
    target_columns = schema_obj.classes ["TableSchema"].slots
    
    properties = {
        "title":       schema_obj.title
      , "atomic_name": schema_obj.name
      , "remote_path": schema_obj.id
      , "description": str (schema_obj.description or "") # empty string meaningful
      , "license":     schema_obj.license
      , "keywords":    schema_obj.keywords
      , "columns":     target_columns
    }
    
    logging.debug (f"Extracted schema properties: {properties}")
    return (properties)

def take (iter : Iterable, n : int, ini : int = 0) -> Iterable:
    '''
    Get the first 'n' characters in an iterable.
    Note, pydantic actually has a type for positive numbers, &c.
    '''
    return (iter [ini:n])
    
def job_table (dataclass
              , manifest  : str   = "manifest.rdf"
              , preamble  : bool  = False
              , mode      : str   = 'w'
              , col_names : tuple[str,  ...] = ("data URI"
                                              , "data schema"
                                              , "data hash")) -> str:
    '''
    Tiny function to pretty-print tables. No need to pull in Pandas just
    to show a really simple JSON object in a table!
    '''
    tables       = dataclass.tables
    tuples       = [(k.resource_path, k.schema_path, k.resource_hash) for k in tables]
    tuples_extra = tuples + [col_names] # Potentially adjust column lengths
    
    file_len = max ([len (p[0]) for p in tuples_extra])
    spec_len = max ([len (p[1]) for p in tuples_extra])
    hash_len = len (col_names [2])
    row_len  = 2 + file_len + 3 + spec_len + 3 + hash_len + 2

    pad_item = lambda k, rl : k + (rl - len(k)) * ' '
    gen_row  = lambda p0, p1, p2, l0, l1, l2 : "".join (["| ", pad_item (p0, l0)
                                                      , " | ", pad_item (p1, l1)
                                                      , " | ", pad_item (p2, l2)
                                                      , " |"])
    border_row = '-' * row_len
    row_title  = gen_row (col_names[0], col_names[1], col_names[2], file_len, spec_len, hash_len)
    rows_body  = [gen_row (k[0], k[1], take(k[2],hash_len), file_len, spec_len, hash_len) for k in tuples]
    table_body = [border_row, row_title, border_row] + rows_body + [border_row]
 
    if (preamble):
        if (mode == 'w'):
            table_lead = f"Wrote to {manifest}:"
        elif (mode == 'r'):
            table_lead = f"Read from {manifest}:"
        else:
            table_lead = f"{manifest}:"
        table_text = '\n'.join ([table_lead] + table_body)
    else:
        table_text = '\n'.join (table_body)
    return (table_text)

class error(object):
    _strict = False
    @classmethod
    def strict(cls, strict):
        cls._strict = strict
    def __init__(self, s):
        """
        Raise an exception if we are in strict mode, otherwise just print
        things like validation errors.
        """
        if self._strict:
            raise Exception(s)
        else:
            print(s)
