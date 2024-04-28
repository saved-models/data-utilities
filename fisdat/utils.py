import codecs
from collections.abc             import Iterable
from itertools                   import chain
from linkml_runtime.dumpers      import RDFLibDumper
from linkml_runtime.linkml_model import SchemaDefinition
from linkml.generators.pythongen import PythonGenerator
from linkml.generators.rdfgen    import RDFGenerator
from linkml.utils.schemaloader   import SchemaLoader
from linkml.validator            import validate_file
import logging
from os import replace
from os.path                     import isfile
from pathlib                     import PurePath
import re
from typing                      import Optional

def fst(g):
    '''
    Utility function. RDFLib gives generators all over the place and
    usually we just want a single value.
    '''
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

def prefix_helper (schema_definition : SchemaDefinition
                 , uriorcurie        : str
                 , base_prefix       : str):
    '''
    Extract prefix and term and expand it

    If the prefix doesn't exist, link to the default.
    If the default isn't set, link to hard-coded `rap' prefix

    The `.default_prefix' field is confusingly a single text code,
    whereas the `.prefixes' field is a map between prefix text codes and
    the reference URI, which is what we're actually interested in.
    '''
    print (f"Called prefix_helper on {uriorcurie} with base prefix {base_prefix}")
    
    default     = schema_definition.default_prefix
    tuple_iri   = uriorcurie.split (':', 1)
    # First case only applies to things referenced locally,
    # use the base prefix!
    if (len (tuple_iri) == 1):
        return (base_prefix + tuple_iri[0])
    else:
        prefix = tuple_iri[0]
        term   = tuple_iri[1]

        # This is the map of all declared prefixes
        prefix_maps = schema_definition.prefixes

        test_target_map = prefix_maps.get (prefix)
                
        if (test_target_map is None and default is None):
            prepend_uri = base_prefix
        elif (test_target_map is None and (prefix_maps.get (default)) is None):
            prepend_uri = base_prefix
        elif (test_target_map is None):
            default_map = prefix_maps.get (default)
            prepend_uri = default_map.prefix_reference
        else:
            target_map = prefix_maps.get (prefix)
            prepend_uri = target_map.prefix_reference

        return (prepend_uri + term)
    
def schema_components_helper (schema_obj) -> dict [str, str]:
    '''
    A shim which serialises the schema proper, to extract components of
    interest, so that they can be serialised in the manifest `tables'
    section.

    As regards to the slots, there are two elements to this.

    First, there are an arbitary number of slots which the schema may
    include. It is possible that some of these are actually top-level
    imports from a different schema.

    Second, there are the slots which are actually used to validate the
    data file, which are properties of the `TableSchema' implementation.

    However, it is this first superset of slots associated with
    `TableSchema' which include any notion of mappings.
    '''
    #logging.debug (f"Calling `schema_components_helper (schema = {schema})'")
    all_slots      = schema_obj.slots.items()
    target_columns = schema_obj.classes ["TableSchema"].slots
    #target_pairs   = {k:(v.exact_mappings, for (k,v) in all_slots if k in target_columns}

    get_mappings = lambda k, v : {
        "name":  k
      , "uri":   v.definition_uri
      , "super": v.is_a
      , "impl":  v.implements
      , "exact": v.exact_mappings
    }
    target_pairs = {k:get_mappings(k,v) for (k,v) in all_slots if k in target_columns}

    properties = {
        "title":       schema_obj.title
      , "atomic_name": schema_obj.name
      , "remote_path": schema_obj.id
      , "description": str (schema_obj.description or "") # empty string meaningful
      , "license":     schema_obj.license
      , "keywords":    schema_obj.keywords
      , "columns":     target_pairs
    }
    logging.debug (f"Extracted schema properties: {properties}")
    return (properties)

def mapping_helper(column_mapping    : dict[str, str]
                 , py_module
                 , py_schema         : SchemaDefinition
                 , target_set_atomic : str
                 , prefixes          : dict[str, str]
    ) -> dict[str, str]:
    '''
    Given a column mapping fetched using `schema_components_helper()',
    summarise it as a `ColumnDesc' object.

    When present, the underlying variable (from LinkML `exact_mappings')
    actually has the prefix `saved'.
    '''
    logging.debug (f"Calling `mapping_helper (column_mapping = {column_mapping}, target_set_atomic = {target_set_atomic})'")
    base_prefix  = prefixes["_base"]
    saved_prefix = prefixes["saved"]
    target_set_uri    = prefix_helper (py_schema, target_set_atomic, base_prefix)
    target_properties = column_mapping [1]
    provenance        = target_properties ["super"]
    column_uri        = target_properties ["uri"]
    exact             = target_properties ["exact"]
    
    if (exact is None or len (exact) == 0):
        underlying = saved_prefix + "some_underlying_variable"
    else:
        underlying = prefix_helper (py_schema, exact[0], base_prefix)

    column_desc = py_module.ColumnDesc (
        column   = column_uri
      , variable = underlying
      , table    = target_set_uri
    )
    return ((provenance, column_desc))

def expand_schema_components(
      py_obj
    , py_schema         : SchemaDefinition
    , schema_properties : dict[str]
    , scoped_columns    : [str]
    , prefixes          : dict[str,str]
    , names_descriptive : [str] = ["column_descriptive", "saved:column_descriptive"]
    , names_collected   : [str] = ["column_collected",   "saved:column_collected"  ]
    , names_modelled    : [str] = ["column_modelled",    "saved:column_modelled"   ]
    ) -> dict[[str]]:
    '''
    This is primarily to avoid boiler-plate which was getting unmanageable in `cmd_dat.py'.
    It's effectively set differences to sort columns
    '''
    base_prefix  = prefixes["_base"]
    saved_prefix = prefixes["saved"]
    
    logging.debug ("Call `expand_schema_components (schema = {schema})'")

    target_set_atomic = schema_properties ["atomic_name"]
    
    gen_dummy_column = lambda k : mapping_helper (k, py_obj, py_schema, target_set_atomic, prefixes)

    if (len (scoped_columns) == 0):
        target_set_columns = list (map (gen_dummy_column, list (schema_properties ["columns"].items ())))
    else:
        union_columns      = set (scoped_columns)  & set (list (schema_properties ["columns"]))
        target_set_columns = list (map (gen_dummy_column, list (union_schema_columns)))
        
    names_all = names_descriptive + names_collected + names_modelled

    logging.info ("Sorting columns into descriptive, collected and modelled lists")
    columns_descriptive = filter (lambda m : fst (m) in names_descriptive, target_set_columns)
    columns_collected   = filter (lambda m : fst (m) in names_collected, target_set_columns)
    columns_modelled    = filter (lambda m : fst (m) in names_modelled, target_set_columns)
    columns_other       = filter (lambda m : fst (m) not in names_all, target_set_columns)

    snd = lambda k : k[1]
    result = {
        "descriptive": list (map (snd, list (columns_descriptive)))
      , "collected":   list (map (snd, list (columns_collected)))
      , "modelled":    list (map (snd, list (columns_modelled)))
      , "other":       list (map (snd, list (columns_other)))
    }
    return (result)

def schema_to_ttl(schema_path       : PurePath
                , ttl_override_path : Optional[PurePath]
                , yaml_rename       : bool = False
    ) -> (bool, Optional[PurePath], Optional[PurePath]):
    '''
    The final step prior to uploading is to convert the schema to an RDF
    representation. This will be helpful for creating an RDF data-set
    later on, and it means that the schemata themselves can be processed
    with non-Python tooling (since the YAML schemata are for the LinkML
    library).

    The general expectation is that the schemata for data files will have
    a `.yaml' extension as advised. However, it is possible that folks
    may get confused, either by passing the wrong file into the command-
    line utilities, or by naming their schema file with a `.ttl' or
    `.rdf' extension.

    In this case, it makes good sense to normalise a given YAML schema to
    have a `.yaml' extension, which introduces a problem when the target
    file to be renamed already exists. The default behaviour must not be
    to forcibly overwrite this, as we judge it to be more likely that the
    wrong filename will be passed into the command-line tools than
    producing a file with the wrong extension.

    The target turtle equivalent will just be devised by substituting the
    extension of the provided schema file (any extension) for `.ttl'.
    The use-case here is the exact opposite because the model of usage of
    the `fisdat' and `fisup' command-line utilities is instead to append
    to a manifest file. As such, if the file exists, rename it so that a
    `.bak' extension is appended to the file name.
    '''
    logging.debug (f"Called `convert_schema_pre_upload (schema_path = {schema_path})'")
    
    schema_stem = schema_path.stem
    schema_ext  = schema_path.suffix

    target_schema_obj   = SchemaLoader (schema_path.name)
    renamed_schema_path = PurePath (schema_stem + ".yaml")

    if (schema_ext == ".ttl" or schema_ext == ".rdf"):
        print (f"Provided schema {schema_path.name} has an RDF (TTL/RDF-XML) extension, attempt to rename")
        
        if (isfile (renamed_schema_path.name) and not yaml_rename):
            print (f"Can't normalise extension of {schema_path.name} to `.yaml' as target file {renamed_schema_path.name} already exists!")
            print ("Override with the `-f' or `--force' flag…")
            actual_schema_path = schema_path
            success            = False
        elif (isfile (renamed_schema_path.name)):
            print (f"Normalising extension of {schema_path.name} to `.yaml' even though target file {renamed_schema_path.name} already exists.")
            actual_schema_path = renamed_schema_path            
            replace (schema_path.name, renamed_schema_path.name)
            success = True
        else:
            print (f"Normalising extension of {schema_path.name} to `.yaml'.")
            actual_schema_path = renamed_schema_path
            replace (schema_path.name, renamed_schema_path.name)
            success = True
    else:
        success            = True
        actual_schema_path = schema_path

    # Always overwritten:
    if (ttl_override_path is not None):
        target_ttl_path = ttl_override_path
    else:
        target_ttl_path = PurePath (schema_stem + ".ttl")
    logging.info (f"Target turtle manifest path is {target_ttl_path}")
        
    if (success):
        if (isfile (target_ttl_path)):
            backup_ttl_path = target_ttl_path.with_suffix (".bak")
            print (f"Target turtle manifest {target_ttl_path} already exists, backed up to {backup_ttl_path}")
            replace (target_ttl_path, backup_ttl_path)
        
        logging.info ("Generating RDF from provided schema")
        generator = RDFGenerator (schema = target_schema_obj.schema)#, schemaview = schema_view)

        logging.info (f"Dumping generated RDF to {target_turtle}")
        ttl_description = generator.serialize()
        output_ttl = codecs.open (target_ttl_path.name, "w", "utf-8")
        output_ttl.write (ttl_description)
        output_ttl.close ()
        
    return (success, actual_schema_path, target_ttl_path)

def malformed_id_helper (manifest_obj
                       , new_identifier : str
                       , base_prefix    : str = "saved") -> (str, str):
    '''
    Peer into the manifest structure to get all atoms in the description

    Whereas they appear as triples at the top level in turtle or NT,
    when reading programmaticaly, they are associated with the class
    instance's position in the [nested] directory structure.

    For example, a given set of job 'sources' are a list associated with
    a job, which is itself part of a list of jobs in the manifest root.

    Prints an error message and returns true/false.
    '''
    root_identifier    = manifest_obj.atomic_name
    table_identifiers  = [tab.atomic_name for tab in manifest_obj.tables]
    job_pairs          = [(job.atomic_name, job.job_sources) for job in manifest_obj.jobs]
    job_identifiers    = [jp[0] for jp in job_pairs]
    sources            = [jp[1] for jp in job_pairs]
    source_identifiers = [src.atomic_name for src in chain (*sources)]
    
    all_identifiers = [[root_identifier]
                      , table_identifiers
                      , job_identifiers
                      , source_identifiers]
    
    regexp     = re.compile (f"^{base_prefix}:([A-z|0-9|_]+)$")
    regexp_spc = re.compile (f".*:.*")
    normalise = lambda a : re.sub (regexp, "\\1", a)
        
    flattened = list (map (normalise, chain (*all_identifiers)))
    
    norm_identifier = normalise (new_identifier)

    if (norm_identifier == new_identifier):
        logging.info ("Identifier was NOT in expected `prefix:identifier' form")
    else:
        logging.info ("Identifier was in expected form")

    if (norm_identifier in flattened):
        logger.info (f"Normalised identifier {norm_identifier} was present in extant top-level identifiers {flattened}, cannot add")
        return (True)
    else:
        logging.info (f"Normalised identifier {norm_identifier} was not present in extant top-level identifiers {flattened}, may add")
        return False

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
