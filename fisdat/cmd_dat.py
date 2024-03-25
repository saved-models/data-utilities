from linkml.generators.pythongen     import PythonGenerator
from linkml.utils.datautils          import _get_context, _get_format, get_dumper, get_loader
from linkml.utils.schema_builder     import SchemaBuilder
from linkml.validator                import validate_file
from linkml.validator.report         import Severity, ValidationResult, ValidationReport
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

import argparse
from hashlib import sha384
from os.path import isfile
from pathlib import Path, PurePath
import tempfile
import logging

from fisdat.utils import fst, error, extension_helper, job_table, take, vprint, vvprint
from fisdat.ns    import CSVW

def validate_wrapper (data         : str
                    , schema       : str
                    , target_class : str
                    , verbosity    : int) -> bool:
    '''
    `validate_file()' either returns an empty list or a collection of
    errors in a report (`linkml.validator.report.ValidationReport').
    
    Setting the `strict' flag means that it fails on the first error,
    so we only get one. I think this behaviour is better as it catches
    the first error and should make it easier to fix.

    Compared to the hideous Python Traceback, these errors are remarkably
    friendly and informative!
    '''
    vvprint (f"Called `validate_wrapper (data = {data}, schema = {schema}, target_class = {target_class})'", verbosity)
    prereq_check = isfile (data) and isfile (schema)

    if (prereq_check):
        report  = validate_file (data, schema, target_class, strict = True)
        results = report.results

        if (not results):
            vprint (f"Validation success: data file {data} against schema file {schema}, with target class {target_class}", verbosity)
            return (True)
        else:
            single_result = results[0]
            severity = single_result.severity
            problem  = single_result.message
            instance = single_result.instance
            
            print ("Validation error: ")
            if (verbosity):
                print (f"-> Data file: {data}")
                print (f"-> Schema file: {schema}")
                print (f"-> Target class: {target_class}")
            print (f"-> Severity: {severity}")
            print (f"-> Message: {problem}")
            print (f"-> Trace: {instance}")
        
            return (False)
    else:
        print (f"Data file {data} and schema file {schema} must exist!")
        return (prereq_check)

def dump_wrapper (py_obj
                , data_model_view : SchemaView
                , output_path     : PurePath
                , verbosity       : int) -> bool:
    '''
    Given a Python object to serialise, and a SchemaView object derived
    from the data model, serialise RDF.
    While `get_dumper' is generic and will happily select the right
    dumper function based on the output path's file extension, the
    RDF and JSON-LD serialisers unfortunately don't accept the same
    schemaview context, instead for JSON-LD, one has to invoke
    `get_contexts' and provide the resulting context to the conexts
    argument.
    There was strange behaviour when calling RDFDumper.dumper directly,
    which is why it's not called directly.
    '''
    vvprint (f"Called `dump_wrapper (py_obj = {py_obj}, data_model_view = {SchemaView}, output_path = {str(output_path)})'", verbosity)
    
    output_path_abs = str (output_path.name)
    output_path_ext = extension_helper (output_path)

    if (output_path_ext != "rdf"):
        vprint (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as RDF/TTL", verbosity)
    
    formatter = _get_format (output_path_abs, "rdf")
    dumper    = get_dumper  (formatter)

    vprint (f"Dumping Python object to {output_path_abs}", verbosity)
    dumper.dump (py_obj, output_path_abs, schemaview = data_model_view)
    return (True)

def append_job_manifest (data       : str
                       , schema     : str
                       , data_model : str
                       , manifest   : str
                       , job_title  : str
                       , mode       : str
                       , verbosity  : int) -> bool:
    '''
    Given a data file, a file schema, and the parent data model, build
    up a Python object which can be serialised to RDF.
    The data model is necessary as it provides JSON-LD contexts, which
    are ncessary when serialising JSON-LD and RDF. It can point to
    job.yaml or the meta-model which pulls it in at the top-level.
    '''
    vvprint (f"Called `append_job_manifest (data = {data}, schema = {schema}, data_model = {data_model}, manifest = {manifest}, job_title = {job_title}, mode = {mode})", verbosity)
    
    data_model_path = PurePath (data_model)
    manifest_path   = PurePath (manifest)
    manifest_ext    = extension_helper (manifest_path)
    data_path       = PurePath (data) # Necessary to only include the file name proper
    schema_path     = PurePath (schema) # ''

    # Note, even before calling this function, the file is known to exist
    with open (data, "rb") as fp:
        data_text = fp.read ()
    data_hash = sha384 (data_text).hexdigest()
    
    # PythonGenerator seems to spit out WARNING messages regardless of log_level
    vprint (f"Creating Python object from data model {data_model}", verbosity)
    py_data_model_base = PythonGenerator (data_model)

    vprint (f"Compiling Python object", verbosity)
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview

    # We've already got the schema, now add the data
    staging_table = py_data_model_module.TableDesc (data_uri    = data_path.name
                                                  , data_schema = schema_path.name
                                                  , data_hash   = data_hash)
    
    if (mode == "initialise"):
        vprint (f"Initialising manifest {manifest}", verbosity)
        manifest_skeleton = py_data_model_module.JobDesc (tables = staging_table)
        result = dump_wrapper (py_obj          = manifest_skeleton
                             , data_model_view = py_data_model_view
                             , output_path     = manifest_path
                             , verbosity       = verbosity)
        print (job_table (manifest_skeleton, manifest, preamble = True))
    else:
        vprint (f"Reading existing manifest {manifest}", verbosity)
        target_class      = py_data_model_module.__dict__["JobDesc"]
        loader            = get_loader  ("rdf")
        original_manifest = loader.load (source       = manifest
                                       , target_class = target_class
                                       , schemaview   = py_data_model_view)

        vprint (f"Checking that data file {data} does not already exist in manifest", verbosity)
        extant_data  = map (lambda k : PurePath (k.data_uri).name, original_manifest.tables)
        check_extant = data_path.name in extant_data
        
        if (check_extant):
            print (f"Data-file {data} was already in the table, cannot add!")
            result = not check_extant
        else:
            vprint (f"Data-file {data} was not in manifest, adding", verbosity)
            staging_tables    = original_manifest.tables + [staging_table]
            manifest_skeleton = py_data_model_module.JobDesc (tables = staging_tables)

            result = dump_wrapper (py_obj          = manifest_skeleton
                                 , data_model_view = py_data_model_view
                                 , output_path     = manifest_path
                                 , verbosity       = verbosity)
            print (job_table (manifest_skeleton, manifest, preamble = True))
    return (result)

def manifest_wrapper (data       : str
                    , schema     : str
                    , data_model : str
                    , manifest   : str
                    , job_title  : str
                    , validate   : bool
                    , verbosity  : int) -> bool:
    '''
    Simple wrapper for the two modes of `append_job_manifest' based on
    whether the manifest file exists (optional) and whether the schema
    and data file exists (obviously mandatory).
    '''
    vvprint (f"Called `manifest_wrapper (data = {data}, schema = {schema}, data_model = {data_model}, manifest = {manifest}, job_title = {job_title}, validate = {validate})", verbosity)
    vvprint (f"Checking that input data {data} and schema {schema} files exist", verbosity)
    
    prereq_check = isfile (data) and isfile (schema)
    
    if (isfile (data) and isfile (schema)):
        if (validate):
            validate_check = validate_wrapper (data, schema, "Column", verbosity)
        else:
            vprint (f"Validation of data-file {data} against schema {schema} disabled", verbosity)
            validate_check = True
            
        if (validate_check):
            if (isfile (manifest)):
                vprint (f"Manifest exists, appending to manifest {manifest}", verbosity)
                result = append_job_manifest (data, schema, data_model, manifest
                                            , job_title, "append", verbosity)
            else:
                vprint (f"Manifest does not exist, creating new manifest {manifest}", verbosity)
                result = append_job_manifest (data, schema, data_model, manifest
                                            , job_title, "initialise", verbosity)
            return (result)
        else:
            '''
            `validate_wrapper' already returns informative error
            messages, so just return its boolean signal
            '''
            return (validate_check)
    else:
        if (not isfile (data) and not isfile (schema)):
            print (f"Neither data file {data} nor schema file {schema} exist!")
        elif (not isfile (data)):
            print (f"Data file {data} does not exist!")
        elif (not isfile (schema)):
            print (f"Schema file {schema} does not exist!")
        return (prereq_check)


    
def cli () -> None:
    parser = argparse.ArgumentParser ("fisdat")
    verbgr = parser.add_mutually_exclusive_group (required = False)
    parser.add_argument ("schema"  , help = "Schema file/URI (YAML)", type = str)
    parser.add_argument ("csvfile" , help = "CSV data file", type = str)
    parser.add_argument ("manifest", help = "Manifest file", type = str)
    parser.add_argument ("-n", "--no-validate", "--dry-run"
                       , help   = "Disable validation"
                       , action = "store_true")
    parser.add_argument ("--data-model"
                       , help    = "Data model YAML specification"
                       , default = str(Path(__file__).parent / "../data-model/src/model/meta.yaml"))
    verbgr.add_argument ("-v", "--verbose"
                       , help = "Show more information about current running state"
                       , required = False
                       , action   = "store_true")
    verbgr.add_argument ("-vv", "--extra-verbose"
                       , help = "Show even more information about current running state"
                       , required = False
                       , action   = "store_true")

    args = parser.parse_args ()

    if (args.verbose):
        verbosity = 1
    elif (args.extra_verbose):
        verbosity = 2
    else:
        verbosity = 0
    
    manifest_wrapper (data       = args.csvfile
                    , schema     = args.schema
                    , data_model = args.data_model
                    , manifest   = args.manifest
                    , job_title  = "saved_job_default"
                    , validate   = not args.no_validate
                    , verbosity  = verbosity)
