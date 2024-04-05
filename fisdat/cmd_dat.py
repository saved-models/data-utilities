from linkml.generators.pythongen     import PythonGenerator
from linkml.utils.schema_builder     import SchemaBuilder
from linkml.validator                import validate_file
from linkml.validator.report         import Severity, ValidationResult, ValidationReport
from linkml_runtime.dumpers          import RDFLibDumper, YAMLDumper
from linkml_runtime.loaders          import RDFLibLoader, YAMLLoader
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

import argparse
from hashlib import sha384
from os.path import isfile
from pathlib import Path, PurePath
import inspect
import logging

from fisdat import __version__, __commit__
from fisdat.utils import fst, error, conversion_shim, extension_helper, job_table, take
from fisdat.ns    import CSVW
from importlib import resources as ir
from . import data_model as dm

def validate_wrapper (data         : str
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

def dump_wrapper (py_obj
                , data_model_view : SchemaView
                , output_path     : PurePath
                , mode            : str) -> bool:
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
    logging.debug (f"Called `dump_wrapper (py_obj = {py_obj}, data_model_view = {SchemaView}, output_path = {str(output_path)}, mode = {mode})'")
    
    output_path_abs = str (output_path.name)
    output_path_ext = extension_helper (output_path)

    if (mode == "rdf_ttl_manifest"):
        if (output_path_ext != "rdf" and output_path_ext != "ttl"):
            logging.info (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as RDF/TTL")
        dumper = RDFLibDumper ()
        
        logging.info (f"Dumping Python object to {output_path_abs}")
        dumper.dump (py_obj, output_path_abs, schemaview = data_model_view)

        return (True)            
        
    elif (mode == "yaml_template"):
        if (output_path_ext != "yaml" and output_path_ext != "yml"):
            logging.info (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as YAML")
        dumper = YAMLDumper ()
        
        logging.info (f"Dumping Python object to {output_path_abs}")
        dumper.dump (py_obj, output_path_abs)

        return (True)
    
    else:
        print ("Unrecognised mode for `dump_wrapper()', cannot dump object")
        return (False)

def append_job_manifest (data           : str
                       , schema         : str
                       , data_model     : str
                       , manifest       : str
                       , manifest_title : str
                       , mode           : str
                       , example_job    : bool) -> bool:
    '''
    Given a data file, a file schema, and the parent data model, build
    up a Python object which can be serialised to RDF.
    The data model is necessary as it provides JSON-LD contexts, which
    are ncessary when serialising JSON-LD and RDF. It can point to
    job.yaml or the meta-model which pulls it in at the top-level.
    '''
    logging.debug (f"Called `append_job_manifest (data = {data}, schema = {schema}, data_model = {data_model}, manifest = {manifest}, manifest_title = {manifest_title}, mode = {mode})'")
    
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
    logging.info (f"Creating Python object from data model {data_model}")
    py_data_model_base = PythonGenerator (data_model)

    logging.info (f"Compiling Python object")
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview

    schema_properties = conversion_shim (schema)
    
    logging.info ("Generating base job description")
    target_set_atomic = schema_properties ["atomic_name"]
    staging_example_job = py_data_model_module.JobDesc (
        title             = f"Empty job template for {target_set_atomic}"
      , atomic_name       = f"job_empty_{target_set_atomic}" # The test job draws from 
      , job_type          = 'ignore'
      , job_auto_generate = True
      , job_sources       = py_data_model_module.SourceDesc (
          atomic_name = target_set_atomic
        , scope       = ['test_col_0', 'test_col_1', 'test_col_2']
      )
    )
    staging_empty_job = py_data_model_module.JobDesc (
        title             = ''
      , atomic_name       = ''
      , job_type          = 'ignore'
      , job_auto_generate = False
      , job_sources       = py_data_model_module.SourceDesc (
          atomic_name = ''
        , scope       = ['']
      )
    )
    if (example_job):
        staging_job = staging_example_job
    else:
        staging_job = staging_empty_job
    
    logging.info ("Generating base table description")
    staging_table = py_data_model_module.TableDesc (
        title       = schema_properties ["title"]
      , atomic_name = schema_properties ["atomic_name"] # $target_set_atomic
      , description = schema_properties ["description"] # Partly for filling out a template, use even empty
      , path        = data_path.name
      , schema_path = schema_path.name
      , hash        = data_hash
      , scope       = ['']
    )
    logging.info ("Proceed with manifest initialise or append operation")
    
    if (mode == "initialise"):
        logging.info (f"Initialising manifest {manifest}")
        manifest_skeleton = py_data_model_module.ManifestDesc (
            tables        = staging_table
          , jobs          = [staging_job]
          , local_version = __version__
        )
        result = dump_wrapper (py_obj          = manifest_skeleton
                             , data_model_view = py_data_model_view
                             , output_path     = manifest_path
                             , mode            = "rdf_ttl_manifest"  )
        
        print (job_table (manifest_skeleton, manifest, preamble = True))

    else:
        logging.info (f"Reading existing manifest {manifest}")
        target_class     = py_data_model_module.ManifestDesc
        loader           = RDFLibLoader ()
        staging_manifest = loader.load (source       = manifest
                                      , target_class = target_class
                                      , schemaview   = py_data_model_view)

        logging.info (f"Checking that data file {data} does not already exist in manifest")
        extant_data  = map (lambda k : PurePath (k.path).name, staging_manifest.tables)
        check_extant = data_path.name in extant_data
        
        if (check_extant):
            print (f"Data-file {data} was already in the table, cannot add!")
            result = not check_extant
        else:
            logging.info (f"Data-file {data} was not in manifest, adding")
            
            # Don't bother adding an additional empty job here, since the 
            # actual aim of creating the empty or example job in the
            # initialisation stage is to make sure that the job field is
            # minimally filled out.
            # These fields are marked as required in `job.yaml', and
            # we've just read in the manifest using that component of the
            # data model as the manifest's *schema*. Therefore, it's safe
            # to assume that they are filled out if we get this far.
            # Further update the local utility version string to that of
            # the most recent time we run it.
            staging_manifest.tables.append (staging_table)
            staging_manifest.local_version = __version__
            
            result = dump_wrapper (py_obj          = staging_manifest
                                 , data_model_view = py_data_model_view
                                 , output_path     = manifest_path
                                 , mode            = "rdf_ttl_manifest")

            print (job_table (staging_manifest, manifest, preamble = True))
            
    return (result)

def manifest_wrapper (data           : str
                    , schema         : str
                    , data_model     : str
                    , manifest       : str
                    , manifest_title : str
                    , validate       : bool
                    , table_class    : str
                    , example_job    : bool) -> bool:
    '''
    Simple wrapper for the two modes of `append_job_manifest' based on
    whether the manifest file exists (optional) and whether the schema
    and data file exists (obviously mandatory).
    '''
    logging.debug (f"Called `manifest_wrapper (data = {data}, schema = {schema}, data_model = {data_model}, manifest = {manifest}, manifest_title = {manifest_title}, validate = {validate})'")
    logging.debug (f"Checking that input data {data} and schema {schema} files exist")
    
    prereq_check = isfile (data) and isfile (schema)
    
    if (isfile (data) and isfile (schema)):
        if (validate):
            validate_check = validate_wrapper (data, schema, table_class)
        else:
            logging.info (f"Validation of data-file {data} against schema {schema} disabled")
            validate_check = True
            
        if (validate_check):
            if (isfile (manifest)):
                logging.info (f"Manifest exists, appending to manifest {manifest}")
                result = append_job_manifest (data, schema, data_model, manifest
                                            , manifest_title, "append", example_job)
            else:
                logging.info (f"Manifest does not exist, creating new manifest {manifest}")
                result = append_job_manifest (data, schema, data_model, manifest
                                            , manifest_title, "initialise", example_job)
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
    print (f"This is fisdat version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser ("fisdat")
    verbgr = parser.add_mutually_exclusive_group (required = False)
    parser.add_argument ("schema"  , help = "Schema file/URI (YAML)", type = str)
    parser.add_argument ("csvfile" , help = "CSV data file", type = str)
    parser.add_argument ("manifest", help = "Manifest file", type = str)
    parser.add_argument ("-n", "--no-validate", "--dry-run"
                       , help   = "Disable validation"
                       , action = "store_true")
    parser.add_argument ("--data-model"
                       , help    = "Data model YAML specification fisdat/data_model/src/model"
                       , default = "meta")
    parser.add_argument ("--table-class"
                       , help    = "Name of LinkML class against which target file is validated"
                       , default = "TableSchema")
    parser.add_argument ("--example-job"
                       , help = "Fill out example job section when generating manifest"
                       , action = "store_true")
    verbgr.add_argument ("-v", "--verbose"
                       , help     = "Show more information about current running state"
                       , required = False
                       , action   = "store_const"
                       , dest     = "log_level"
                       , const    = logging.INFO)
    verbgr.add_argument ("-vv", "--extra-verbose"
                       , help     = "Show even more information about current running state"
                       , required = False
                       , action   = "store_const"
                       , dest     = "log_level"
                       , const    = logging.DEBUG)

    args = parser.parse_args ()

    logging.basicConfig (level = args.log_level)
            
    logging.debug (f"Polling data model directory")
    root_dir = ir.files (dm)
    logging.debug (f"Data model working directory is: {root_dir}")
    yaml_sch = f"src/model/{args.data_model}.yaml"
    logging.debug (f"Data model path is: {yaml_sch}")
    
    data_model_path = root_dir / yaml_sch
    data_model = str (data_model_path)


    manifest_wrapper (data           = args.csvfile
                    , schema         = args.schema
                    , data_model     = data_model
                    , manifest       = args.manifest
                    , manifest_title = "saved_job_default"
                    , validate       = not args.no_validate
                    , table_class    = args.table_class
                    , example_job    = args.example_job)
