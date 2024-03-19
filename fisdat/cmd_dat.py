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

from fisdat.utils import fst, error, extension_helper, job_table, take
from fisdat.ns    import CSVW

def validate_wrapper (target : str, against : str, target_class : str = "Column") -> bool:
    '''
    `validate_file()' either returns an empty list or a collection of
    errors in a report (`linkml.validator.report.ValidationReport').
    
    Setting the `strict' flag means that it fails on the first error,
    so we only get one. I think this behaviour is better as it catches
    the first error and should make it easier to fix.

    Compared to the hideous Python Traceback, these errors are remarkably
    friendly and informative!
    '''
    prereq_check = isfile (target) and isfile (against)

    if (prereq_check):
        report  = validate_file (target, against, target_class, strict = True)
        results = report.results

        if (not results):
            print ("Validation success")
            return (True)
        else:
            single_result = results[0]
            severity = single_result.severity
            problem  = single_result.message
            instance = single_result.instance
        
            print ("Validation error:")
            print (f"-> Severity: {severity}")
            print (f"-> Message: {problem}")
            print (f"-> Trace: {instance}")
        
            return (False)
    else:
        print (f"Data file {target} and schema file {against} must exist!")
        return (prereq_check)

def dump_wrapper (py_obj
                , data_model_view : SchemaView
                , output_path     : PurePath) -> bool:
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
    output_path_abs = str (output_path.name)
    output_path_ext = extension_helper (output_path)

    if (output_path_ext != "rdf"):
        print (f"Warning: target extension has a .{output_path_ext} extension, but will actually be serialised as RDF")
    
    formatter = _get_format (output_path_abs, "rdf")
    dumper    = get_dumper  (formatter)

    dumper.dump (py_obj, output_path_abs, schemaview = data_model_view)
    return (True)

def append_job_manifest (data       : str
                       , schema     : str
                       , data_model : str
                       , manifest   : str
                       , job_title  : str
                       , mode       : str) -> bool:
    '''
    Given a data file, a file schema, and the parent data model, build
    up a Python object which can be serialised to RDF.
    The data model is necessary as it provides JSON-LD contexts, which
    are ncessary when serialising JSON-LD and RDF. It can point to
    job.yaml or the meta-model which pulls it in at the top-level.
    '''
    data_model_path = PurePath (data_model)
    manifest_path   = PurePath (manifest)
    manifest_ext    = extension_helper (manifest_path)
    data_path       = PurePath (data) # Necessary to only include the file name proper
    schema_path     = PurePath (schema) # ''

    # Note, even before calling this function, the file is known to exist
    with open (data, "rb") as fp:
        data_text = fp.read ()
    data_hash = sha384 (data_text).hexdigest()
    
    # Nice!
    py_data_model_base   = PythonGenerator (data_model)
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview

    # We've already got the schema, now add the data
    staging_table = py_data_model_module.TableDesc (data_uri    = data_path.name
                                                  , data_schema = schema_path.name
                                                  , data_hash   = data_hash)
    
    if (mode == "initialise"):
        manifest_skeleton = py_data_model_module.JobDesc (tables = staging_table)
        result = dump_wrapper (py_obj          = manifest_skeleton
                             , data_model_view = py_data_model_view
                             , output_path     = manifest_path)
        print (job_table (manifest_skeleton, manifest, preamble = True))
    else:
        target_class      = py_data_model_module.__dict__["JobDesc"]
        loader            = get_loader  ("rdf")
        original_manifest = loader.load (source       = manifest
                                       , target_class = target_class
                                       , schemaview   = py_data_model_view)
        
        extant_data  = map (lambda k : PurePath (k.data_uri).name, original_manifest.tables)
        check_extant = data_path.name in extant_data
        
        if (check_extant):
            print ("Data-file " + data + " is already in the table, cannot add!")
            result = not check_extant
        else:        
            staging_tables    = original_manifest.tables + [staging_table]
            manifest_skeleton = py_data_model_module.JobDesc (tables = staging_tables)

            result = dump_wrapper (py_obj          = manifest_skeleton
                                 , data_model_view = py_data_model_view
                                 , output_path     = manifest_path)
            print (job_table (manifest_skeleton, manifest, preamble = True))
    return (result)

def manifest_wrapper (data       : str
                    , schema     : str
                    , data_model : str
                    , manifest   : str
                    , job_title  : str
                    , validate   : bool) -> bool:
    '''
    Simple wrapper for the two modes of `append_job_manifest' based on
    whether the manifest file exists (optional) and whether the schema
    and data file exists (obviously mandatory).
    '''
    prereq_check = isfile (data) and isfile (schema)
    
    if (prereq_check):
        if (validate):
            validate_check = validate_wrapper (data, schema)
        else:
            validate_check = True
            
        if (validate_check):
            if (isfile (manifest)):
                result = append_job_manifest (data, schema, data_model, manifest, job_title
                                            , mode="append")
            else:
                result = append_job_manifest (data, schema, data_model, manifest, job_title
                                            , mode="initialise")
            return (result)
        else:
            '''
            `validate_wrapper' already returns informative error
            messages, so just return its boolean signal
            '''
            return (validate_check)
    else:
        print (f"Data file {data} and schema file {schema} must exist!")
        return (prereq_check)


    
def cli () -> None:
    parser = argparse.ArgumentParser ("fisdat")
    parser.add_argument ("schema"  , help = "Schema file/URI (YAML)", type = str)
    parser.add_argument ("csvfile" , help = "CSV data file", type = str)
    parser.add_argument ("manifest", help = "Manifest file", type = str)
    parser.add_argument ("-n", "--no-validate", "--dry-run"
                       , help   = "Disable validation"
                       , action = "store_true")
    parser.add_argument ("--data-model"
                       , help    = "Data model YAML specification"
                       , default = "examples/linkml-scratch/working/src/model/job.yaml"
                       , action  = "store_true")
    
    args = parser.parse_args ()

    manifest_wrapper (data       = args.csvfile
                    , schema     = args.schema
                    , data_model = args.data_model
                    , manifest   = args.manifest
                    , job_title  = "saved_job_default"
                    , validate   = not args.no_validate)
        
def old_cli():
    """
    Command line interface
    """
    parser = argparse.ArgumentParser("fisdat")
    parser.add_argument("-s", "--strict", action="store_true", help="Strict validation")
    parser.add_argument(
        "-u", "--unsecure", action="store_true", help="Disable SSL validation"
    )
    parser.add_argument("-n", "--novalidate", action="store_true", help="Disable validation")
    parser.add_argument("schema", help="Schema file/URI")
    parser.add_argument("csvfile", help="CSV data file")
    parser.add_argument("manifest", help="Manifest file")

    args = parser.parse_args()

    #if args.unsecure:
    #    from rdflib import _networking
    #    from fisdat import kludge
    #
    #    _networking._urlopen = kludge._urlopen

    error.strict(args.strict)

    # Load the schema
    schema = Graph().parse(location=args.schema, format="json-ld")
    # print(schema.serialize(format="n3"))

    if not args.novalidate:
        old_validate(schema, args.csvfile)

    with open(args.csvfile, "rb") as fp:
        data = fp.read()
    hash = sha384(data)

    ## Add the CSV file and its schema to the manifest
    manifest = {"@context": str(CSVW)[:-1], "tables": []}

    # Read the manifest if it exists
    if isfile(args.manifest):
        with open(args.manifest) as fp:
            manifest.update(json.load(fp))

    # Keep any other tables already present
    manifest["tables"] = [t for t in manifest["tables"] if t.get("url") != args.csvfile]
    # Add this table and its schema to the manifest
    manifest["tables"].append({"url": args.csvfile, "tableSchema": args.schema, "fileHash": hash.hexdigest()})

    # Save the new manifest
    with open(args.manifest, "w+") as fp:
        json.dump(manifest, fp, indent=4)
