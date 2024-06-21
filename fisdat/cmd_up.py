from rdflib            import Graph, Namespace, Literal
from rdflib.collection import Collection
from datetime          import datetime
from google.cloud      import storage
from google.cloud      import client as gc
from hashlib           import sha384
import argparse
import codecs
import json
import logging
from os.path import isfile, basename, dirname
from os      import chdir
from pathlib import Path, PurePath
import time
import uuid

import rdflib.plugins.parsers.notation3
import yaml.scanner

from linkml.generators.pythongen     import PythonGenerator
from linkml.generators.rdfgen        import RDFGenerator
from linkml.utils.schema_builder     import SchemaBuilder
from linkml.utils.schemaloader       import SchemaLoader
from linkml.validator                import validate_file
from linkml.validator.report         import Severity, ValidationResult, ValidationReport
from linkml_runtime.dumpers          import RDFLibDumper, YAMLDumper
from linkml_runtime.loaders          import RDFLibLoader, YAMLLoader
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

from fisdat            import __version__, __commit__
from fisdat.ns         import CSVW
from fisdat.utils      import error, fst, extension_helper, prefix_helper, job_table
from fisdat.data_model import ManifestDesc

## data read/write buffer size, 1MB
BUFSIZ=1048576

def upload_files (args    : [str]
                , files   : [str]
                , owner   : str
                , ts      : str
                , dry_run : bool) -> str:
    logging.debug (f"Called `upload_files (args = {args}, files = {files}, owner = {owner}, ts = {ts})'")
    
    gen_path = lambda owner, ts, extra : owner + "/" + ts + "/" + extra
    client   = storage.Client()
    bucket   = client.bucket(args.bucket)
    jobuuid  = str(uuid.uuid1())
    path     = gen_path (owner, ts, args.directory) if args.directory is not None else gen_path (owner, ts, jobuuid)
    if (not dry_run):
        for fname in files:
            fpath = path + "/" + fname
            print (f"Uploading gs://{args.bucket}/{fpath} ...")
            start = time.time ()
            blob = bucket.blob(fpath)
            blob.upload_from_filename (fname, timeout=86400)
            end = time.time ()
            abs_time = end - start
            if (abs_time < 1):
                elapsed = round (abs_time, 2)
            else:
                elapsed = round (abs_time)
            print (f"Uploaded {fname} in {elapsed}s")
    else:
        for fname in files:
            fpath = path + "/" + fname
            print (f"Would upload to gs://{args.bucket}/{fpath} ...")
    return f"gs://{args.bucket}/{path}"

def source () -> str:
    logging.debug ("Called `source()'")
    from google.cloud import client
    c   = client.Client ()
    res = c._credentials.service_account_email
    logging.info (f"GCP account e-mail: {res}")
    return res

def prep_index (manifest_path_yaml : str
              , manifest_path_ttl  : str
              , manifest_name      : str
              , base_prefix        : str
              , index_name         : str
    ) -> str:
    '''
    Echo the manifest file name to .index or other file.
    This avoids hard-coding the manifest title.
    '''
    logging.debug (f"Called `prep_index (manifest_path_yaml = {manifest_path_yaml}, manifest_path_ttl = {manifest_path_ttl}, manifest_name = {manifest_name}, index_name = {index_name})'")

    index_contents = f"{manifest_path_yaml}\n{manifest_path_ttl}\n{base_prefix}\n{manifest_name}"
    
    output_index = codecs.open (index_name, "w", "utf-8")
    output_index.write (index_contents)
    output_index.close ()

    return (index_name)

# To-do: add fallback? Not added since it complicates things
def convert_feasibility (input_path : PurePath
                       , target_ext : str
                       , force      : bool = True) -> bool:
    '''
    Helper function which returns whether a given filesystem operation is
    feasible.

    This doesn't do the write operation proper because it is useful both
    when renaming a file extension named wrongly (YAML data named `.ttl'
    or similar), and when dumping to a target file.
    
    It is unclear about whether some sort of fallback behaviour is desirable
    '''
    logging.debug (f"Called `convert_feasibility (input_path = {input_path}, target_ext = {target_ext}, force = {force})'")
    input_stem  = input_path.stem
    input_ext   = extension_helper (input_path)
    target_path = f"{input_stem}.{target_ext}"
    
    if (target_ext == input_ext):
        print (f"Target extension {target_ext} is same as input, won't do anything")
        stage_write = (False, target_path)
    elif (not isfile (target_path)):
        print (f"Target file {target_path} doesn't exist, so overwrite")
        stage_write = (True, target_path)
    elif (isfile (target_path) and force):
        print (f"Target file {target_path} exists, and force-overwrite is set, so overwrite")
        stage_write = (True, target_path)
    else: #elif (isfile (target_file) and not force):
        print (f"Target file {target_path} exists, but force-overwrite is not set, so don't overwrite!")
        stage_write = (False, target_path)

    return (stage_write)

def coalesce_schema (schema_path_yaml : str, dry_run : bool, force : bool) -> (bool, str):
    '''
    Convert YAML schema to turtle equvialent
    '''
    logging.debug (f"Called `coalesce_schema (schema_path_yaml = {schema_path_yaml}'")

    (feasible, schema_path_ttl) = convert_feasibility (input_path = PurePath (schema_path_yaml)
                                                     , target_ext = "ttl"
                                                     , force      = force)
    if (dry_run and feasible):
        print (f"Would have converted schema from YAML {schema_path_yaml} to TTL {schema_path_ttl}")
        return (True, schema_path_ttl)
    elif (feasible):
        print (f"Proceed with loading schema {schema_path_yaml}")
        target_schema_obj = SchemaLoader (schema_path_yaml)
        print ("Generating RDF from provided schema")
        generator = RDFGenerator (schema = target_schema_obj.schema)#, schemaview = schema_view)
        print ("Done generating RDF from provided schema, serialising")
        schema_ttl_description = generator.serialize()
        print (f"Dumping generated RDF to {schema_path_ttl}")

        schema_output_ttl = codecs.open (schema_path_ttl, "w", "utf-8")
        schema_output_ttl.write (schema_ttl_description)
        schema_output_ttl.close ()

        return (True, schema_path_ttl)
    else:
        print (f"Conversion of schema from YAML {schema_path_yaml} to TTL {schema_path_ttl} is not feasible!")
        return (False, schema_path_ttl)

def coalesce_manifest (manifest_path      : str
                     , manifest_format    : str
                     , data_model_uri     : str
                     , prefixes           : dict[str, str]
                     , gcp_source         : str
                     , dry_run            : bool
                     , force              : bool) -> (bool, ManifestDesc, str, str, str, str):
    '''
    The YAML files are provided and edited locally, but we can't process
    these with non-Python tooling. This function converts schemata
    described in the manifest to turtle, then converts the manifest
    itself to turtle.
    '''
    logging.debug (f"Called `coalesce_manifest (manifest_path = {manifest_path}, data_model_uri = {data_model_uri}, prefixes = {prefixes}, gcp_source = {gcp_source})'")
    
    if not isfile (manifest_path):
        error(f"No such file: {manifest_path}")

    py_data_model_view = SchemaView (data_model_uri)
    
    # Start by loading the manifest
    if (manifest_format == "ttl"):
        loader = RDFLibLoader ()
        dumper = YAMLDumper   ()
        try:
            manifest_obj = loader.load (source       = manifest_path
                                      , target_class = ManifestDesc
                                      , schemaview   = py_data_model_view)
            manifest_path_ttl = manifest_path
            (manifest_feasible, manifest_path_yaml) = convert_feasibility (
                input_path = PurePath (manifest_path)
              , target_ext = "yaml"
              , force      = force # More likely mistaken, don't forcibly overwrite!
            )
        except rdflib.plugins.parsers.notation3.BadSyntax:
            raise ValueError(f"Cannot load file {manifest_path} with the RDF/TTL loader. Is your manifest a YAML manifest? (\"yaml\" `--serialisation' option)")

    elif (manifest_format == "yaml"):
        loader = YAMLLoader   ()
        dumper = RDFLibDumper ()

        try:
            manifest_obj = loader.load (source       = manifest_path
                                        , target_class = ManifestDesc)

            manifest_path_yaml = manifest_path
            (manifest_feasible, manifest_path_ttl) = convert_feasibility (
                input_path = PurePath (manifest_path)
              , target_ext = "ttl"
              , force      = True
            )
        except yaml.scanner.ScannerError:
            raise ValueError (f"Cannot load file {manifest_path} with the YAML loader. Is your manifest an RDF/TTL manifest? (\"ttl\" `--serialisation' option)")
    else:
        raise ValueError (f"Unrecognised serialisation mode {manifest_format}, cannot load extant object")
        
    manifest_name = prefix_helper (py_data_model_view.schema,
                                   manifest_obj.atomic_name,
                                   prefixes["_base"])
    logging.debug(f"Manifest name is {manifest_name}")

    # Check early that the two operations are feasible (for returning if --dry-run)
    
    
    # Order slightly iffy, but schema conversion is not a prerequisite of the manifest conversion
    if (dry_run):
        for tab in manifest_obj.tables:
            (schema_success, path_ttl) = coalesce_schema (tab.schema_path_yaml, dry_run = dry_run, force = False)
            tab.schema_path_ttl        = path_ttl
            
        if (manifest_feasible):
            return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
        else:
            return (False, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
    else:
        for table in manifest_obj.tables:
            # Start by subbing in successfully-serialised TTL schemata
            # Useful print statements in `coalesce_schema()' function
            (schema_success, path_ttl) = coalesce_schema (table.schema_path_yaml, dry_run = dry_run, force = force)
            if (schema_success):
                table.schema_path_ttl = path_ttl

            # Now check hashes of resources
            table_uri = table.resource_path
            print (f"Checking {table_uri} ...")

            prereq_check = isfile (table_uri)
        
            if (not prereq_check):
                print (f"Error: target file {table_uri} does not exist")
            else:
                with open (table_uri, "rb") as fp:
                    data = fp.read ()
                    hash = sha384  (data)
                
                    if hash.hexdigest() != table.resource_hash:
                        raise ValueError(f"{table_uri} has changed, please revalidate with `fisdat'")
                
        # Proceed to conversion of manifest object proper to 
        if (manifest_feasible):
            if (dry_run):
                print (f"Would have converted manifest from YAML {manifest_path_yaml} to TTL {manifest_path_ttl}")
            else:
                # Equivalent of dumping JSON in the old CLI:
                print (job_table (manifest_obj, preamble = False, mode = 'r'))

            # Not actually implemented in data model yet
            #manifest_obj.source = gcp_source

            # We've already caught the exceptions above, so these shouldn't fail. If they do, it's unlikely to be because of the format, so leave as-is.
            if (manifest_format == "ttl"):
                dumper.dump (manifest_obj, manifest_path_yaml
                           , schemaview = py_data_model_view)
                return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
            else:
                dumper.dump (manifest_obj, manifest_path_ttl
                           , schemaview = py_data_model_view
                           , prefix_map = prefixes)
                return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
            
        else:
            print ("Conversion of YAML manifest object to TTL was not successful!")
            return (False, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_name)
    
def cli () -> None:
    """
    Command line interface
    """
    print (f"This is fisup version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser("fisup")
    verbgr = parser.add_mutually_exclusive_group (required = False)

    parser.add_argument ("-u", "--unsecure"
                       , action="store_true"
                       , help="Disable SSL validation")
    parser.add_argument ("-b", "--bucket"
                       , default="saved-fisdat"
                       , help="Bucket to upload into")
    parser.add_argument ("-d", "--directory"
                       , help="Directory within bucket to upload into"
                       , default = None)
    parser.add_argument ("-s", "--source"
                       , help="Data source email"
                       , default = None)
    parser.add_argument ("manifest", help="Manifest file")
    parser.add_argument ("--index"
                       , help = "Name of hidden index file recording manifest file name"
                       , default = ".index")
    parser.add_argument ("--data-model-uri"
                       , help     = "Data model YAML specification URI"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/meta.yaml")
    parser.add_argument ("--manifest-format", "--input-format", "--serialisation"
                       , help     = "Input manifest format"
                       , type     = str
                       , choices  = ["yaml", "ttl"]
                       , default  = "yaml")
    parser.add_argument ("--base-prefix"
                       , help     = "RDF `@base' prefix from which manifest, results, data and descriptive statistics may be served."
                       , default  = "https://marine.gov.scot/metadata/saved/rap/")
    parser.add_argument ("--saved-prefix"
                       , help     = "RDF `saved' data model schema prefix"
                       , default  = "https://marine.gov.scot/metadata/saved/schema/")
    parser.add_argument ("-n", "--no-upload", "--dry-run"
                       , help     = "Don't upload files"
                       , action   = "store_true")
    parser.add_argument ("-f", "--force"
                       , help = "Forcibly overwrite files in case of conflicts"
                       , action = "store_true"
                       , default = False)
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
    
    logging.basicConfig (level  = args.log_level
                       , format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'")
        
    if args.unsecure:
        from rdflib import _networking
        from fisdat import kludge

        _networking._urlopen = kludge._urlopen

    # Sub this into `coalesce_manifest()'
    if (args.source is None):
        data_source_email = source ()
    else:
        data_source_email = args.source

    prefixes = { "_base": args.base_prefix
               , "saved": args.saved_prefix}

    (test_manifest, manifest_obj, manifest_yaml, manifest_ttl, manifest_name) = coalesce_manifest (
            manifest_path      = args.manifest
          , manifest_format    = args.manifest_format
          , data_model_uri     = args.data_model_uri
          , prefixes           = prefixes
          , gcp_source         = data_source_email
          , dry_run            = args.no_upload
          , force              = args.force
        )

    index = prep_index (manifest_path_yaml = manifest_yaml
                      , manifest_path_ttl  = manifest_ttl
                      , manifest_name      = manifest_name
                      , base_prefix        = args.base_prefix
                      , index_name         = args.index)
    
    if (test_manifest):
        resources     = [table.resource_path    for table in manifest_obj.tables]
        schemata_ttl  = [table.schema_path_ttl  for table in manifest_obj.tables]
        schemata_yaml = [table.schema_path_yaml for table in manifest_obj.tables]
        time_stamp    = datetime.today ().strftime ('%Y%m%d')
        short_name    = data_source_email.split ('@') [0]

        staging_files = [manifest_yaml
                       , manifest_ttl
                       , index] + resources + schemata_yaml + schemata_ttl
        
        url = upload_files (args, staging_files, short_name, time_stamp, args.no_upload)

        if (not args.no_upload):
            print(f"Successfully uploaded your data/job set/bundle to {url}")
        else:
            print(f"Would have uploaded your data/job set/bundle to {url}")
