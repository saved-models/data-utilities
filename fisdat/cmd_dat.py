from rdflib import Graph, Namespace, Literal
from rdflib.collection import Collection
from rdflib.term import XSDToPython

from linkml.generators.pythongen import PythonGenerator
from linkml.utils.datautils import _get_context, _get_format, get_dumper, get_loader
from linkml.utils.schema_builder import SchemaBuilder
from linkml.validator import validate, validate_file
from linkml.validator.report import Severity, ValidationResult, ValidationReport
from linkml_runtime.dumpers import JSONDumper, RDFDumper
from linkml_runtime.loaders import YAMLLoader, RDFLoader
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

from hashlib import sha384
from csvwlib.utils.TypeConverter import TypeConverter
import csv
import argparse
from os.path import isfile
from pathlib import Path, PurePath # Necessary to make sure that we don't upload files starting with `..', `.', `/' &c.
import json

from fisdat.utils import fst, error
from fisdat.ns import CSVW

def new_validate (target : str, against : str, target_class : str = "Column") -> bool:
    '''
    `validate_file()' either returns an empty list or a collection of
    errors in a report (`linkml.validator.report.ValidationReport').
    
    Setting the `strict' flag means that it fails on the first error,
    so we only get one. I think this behaviour is better as it catches
    the first error and should make it easier to fix.

    Compared to the hideous Python Traceback, these errors are remarkably
    friendly and informative!
    '''
    report  = validate_file (target, against, target_class, strict=True)
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
        print ("-> Severity: " + severity)
        print ("-> Message: " + problem)
        print ("-> Trace: " + str(instance))
        
        return (False)

def py_dump_wrapper (spec_py_obj
                   , schema_view : SchemaView
                   , output_path : PurePath) -> bool:
    output_path_abs = str (output_path)
    output_path_ext = output_path.suffix [1: len(output_path.suffix)]

    formatter = _get_format (output_path_abs, output_path_ext)
    dumper    = get_dumper  (formatter)

    dumper.dump (spec_py_obj, output_path_abs, schemaview = schema_view)
    return (True)

def dump_wrapper (spec_path    : PurePath
                , schema_path  : PurePath
                , schema_dict  : dict
                , target_class : str
                , output_path  : PurePath) -> bool:

    spec_path_abs   = str (spec_path)
    schema_path_abs = str (schema_path)
    output_path_abs = str (output_path)

    py_target_class = schema_dict [target_class]

    loader     = YAMLLoader  ()
    spec_obj   = loader.load (spec_path_abs, py_target_class)
    schema_obj = SchemaView  (schema_path_abs)

    py_dump_wrapper (spec_obj, schema_obj, output_path)
        
    return (True)
    
# Step 1: Read an initial job spec, and validate each item in it.
def serialise_initial_job_spec (spec_path   : str
                              , schema_path : str = "examples/linkml-scratch/working/src/model/job.yaml"
                              , job_class   : str = "JobDesc"
                              , out_ext     : str = "rdf"
                              , out_file    : str = "manifest.rdf") -> bool:
    job_spec_test = new_validate (spec_path, schema_path, job_class)
    if (job_spec_test):
        py_schema_dict = PythonGenerator (schema_path).compile_module ().__dict__
        dump_wrapper (spec_path    = PurePath (spec_path)
                    , schema_path  = PurePath (schema_path)
                    , schema_dict  = py_schema_dict
                    , target_class = job_class
                    , output_path  = PurePath (out_file))
    return (job_spec_test)
    
# As above except build up the Python object using the linkML API *alone*
# However, previous function was useful for working out how conversion can be done programmatically
def compose_job_spec (data      : str
                    , spec      : str
                    , schema    : str = "examples/linkml-scratch/working/src/model/job.yaml"
                    , manifest  : str = "manifest.rdf"
                    , job_title : str = "abacus"
                    , mode      : str = "initialise") -> bool:
    
    schema_path   = PurePath (schema) # schema_base = schema_path.name
    manifest_path = PurePath (manifest)
    data_path     = PurePath (data)
    spec_path     = PurePath (spec)

    # Nice!
    py_schema_base   = PythonGenerator (schema)
    py_schema_module = py_schema_base.compile_module ()
    py_schema_view   = py_schema_base.schemaview

    # We've already got the schema, now add the data
    staging_table = py_schema_module.TableDesc (url = data, tableSchema = spec)
        
    if (mode == "initialise"):
        manifest_skeleton = py_schema_module.JobDesc (tables = staging_table)
        print (manifest_skeleton)
        py_dump_wrapper (spec_py_obj = manifest_skeleton
                       , schema_view = py_schema_view
                       , output_path = manifest_path)
        return (True)
    else:
        target_class      = py_schema_module.__dict__["JobDesc"]
        loader = get_loader ("rdf")
        original_manifest = loader.load (source = manifest
                                       , target_class = target_class
                                       , schemaview = py_schema_view)
        
        extant_data = map (lambda k : PurePath (k.url).name, original_manifest.tables)

        if (data_path.name in extant_data):
            print ("Data-file " + data + " is already in the table, cannot add!")
            return (False)
        else:        
            staging_tables    = original_manifest.tables + [staging_table]
            manifest_skeleton = py_schema_module.JobDesc (tables = staging_tables)
            print (manifest_skeleton)
            py_dump_wrapper (spec_py_obj = manifest_skeleton
                           , schema_view = py_schema_view
                           , output_path = manifest_path)
            return (True)

def cli() -> None:
    parser = argparse.ArgumentParser ("fisdat")
    parser.add_argument ("schema", help = "Schema (YAML)")
    parser.add_argument ("csvfile", help = "CSV data")
    parser.add_argument ("manifest", help="Manifest file")
    parser.add_argument ("mode", help = "API test mode", default = "validate")
    
    args = parser.parse_args ()

    if (args.mode == "validate"):
        new_validate (args.schema, "examples/linkml-scratch/working/src/model/job.yaml")
    elif (args.mode == "dumpgen"):
        compose_job_spec (data = args.csvfile, spec = args.schema, mode = "initialise")
    elif (args.mode == "dumpappend"):
        compose_job_spec (data = args.csvfile, spec = args.schema, mode = "append")

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
