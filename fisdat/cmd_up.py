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
from typing import Optional
import uuid

import rdflib.plugins.parsers.notation3
import urllib.error
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
            if fname is not None:
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
            if fname is not None:
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
              , manifest_uri       : str
              , base_prefix        : str
              , index_name         : str
    ) -> str:
    '''
    Echo the manifest file name to .index or other file.
    This avoids hard-coding the manifest title.
    '''
    logging.debug (f"Called `prep_index (manifest_path_yaml = {manifest_path_yaml}, manifest_path_ttl = {manifest_path_ttl}, manifest_uri = {manifest_uri}, index_name = {index_name})'")

    index_contents = f"{manifest_path_yaml}\n{manifest_path_ttl}\n{base_prefix}\n{manifest_uri}"

    output_index = codecs.open (index_name, "w", "utf-8")
    output_index.write (index_contents)
    output_index.close ()

    return (index_name)

# To-do: add fallback? Not added since it complicates things
def convert_feasibility (input_path  : str
                       , target_ext  : str
                       , target_path : Optional[str] = None
                       , force       : bool = True) -> (bool, PurePath):
    '''
    Helper function which returns whether a given filesystem operation is
    feasible.

    This doesn't do the write operation proper because it is useful both
    when renaming a file extension named wrongly (YAML data named `.ttl'
    or similar), and when dumping to a target file.
    
    It is unclear about whether some sort of fallback behaviour is desirable
    '''
    input_path_pure = PurePath (input_path)

    if target_path is None:
        logging.debug (f"Called `convert_feasibility (input_path = {input_path}, target_ext = {target_ext}, force = {force})'")
        output_path_pure = PurePath (input_path_pure.with_suffix (f".{target_ext}"))
    else:
        # Don't bother echoing `target_ext' as we don't use it if `target_path' exists
        logging.debug (f"Called `convert_feasibility (input_path = {input_path}, target_path = {target_path}, force = {force})'")
        output_path_pure = PurePath (target_path)
        
    if (output_path_pure.suffix == input_path_pure.suffix):
        print (f"Target extension {target_ext} is same as input, won't do anything")
        stage_write = (False, output_path_pure)
    elif (not isfile (output_path_pure)):
        print (f"Target file {output_path_pure} doesn't exist, so overwrite")
        stage_write = (True, output_path_pure)
    elif (isfile (output_path_pure) and force):
        print (f"Target file {output_path_pure} exists, and force-overwrite is set, so overwrite")
        stage_write = (True, output_path_pure)
    else: #elif (isfile (target_file) and not force):
        print (f"Target file {output_path_pure} exists, but force-overwrite is not set, so don't overwrite!")
        stage_write = (False, output_path_pure)

    return (stage_write)

def coalesce_schema (schema_path_yaml : str, dry_run : bool, force : bool, schema_path_ttl : Optional[str] = None) -> (bool, PurePath):
    '''
    Convert YAML schema to turtle equvialent
    '''
    logging.debug (f"Called `coalesce_schema (schema_path_yaml = {schema_path_yaml}'")

    (feasible, target_path_ttl) = convert_feasibility (input_path  = schema_path_yaml
                                                     , target_path = schema_path_ttl
                                                     , target_ext  = "ttl"
                                                     , force       = force)
        
    if (dry_run and feasible):
        print (f"Would have converted schema from YAML {schema_path_yaml} to TTL {target_path_ttl}")
        return (True, target_path_ttl)
    elif (feasible):
        print (f"Proceed with loading schema {schema_path_yaml}")
        try:
            target_schema_obj = SchemaLoader (schema_path_yaml)
            print ("Generating RDF from provided schema")
            generator = RDFGenerator (schema = target_schema_obj.schema)#, schemaview = schema_view)
            print ("Done generating RDF from provided schema, serialising")
            schema_ttl_description = generator.serialize()
        
            print (f"Dumping generated RDF to {target_path_ttl}")

            schema_output_ttl = codecs.open (target_path_ttl, "w", "utf-8")
            schema_output_ttl.write (schema_ttl_description)
            schema_output_ttl.close ()
 
            return (True, target_path_ttl)
        except yaml.scanner.ScannerError:
            print (f"Conversion of YAML schema {schema_path_yaml} to TTL {target_path_ttl} is not feasible. Is it a valid YAML file?")
            return (False, target_path_ttl)
    else:
        print (f"Conversion of schema from YAML {schema_path_yaml} to TTL {target_path_ttl} is not feasible!")
        return (False, target_path_ttl)


    
def coalesce_manifest (manifest_path      : str
                     , manifest_format    : str
                     , data_model_uri     : str
                     , prefixes           : dict[str, str]
                     , gcp_source         : str
                     , dry_run            : bool
                     , force              : bool
                     , fake_cwd           : str = "") -> (bool, ManifestDesc, str, str, str, str):
    '''
    The YAML files are provided and edited locally, but we can't process
    these with non-Python tooling. This function converts schemata
    described in the manifest to turtle, then converts the manifest
    itself to turtle.
    '''
    logging.debug (f"Called `coalesce_manifest (manifest_path = {manifest_path}, data_model_uri = {data_model_uri}, prefixes = {prefixes}, gcp_source = {gcp_source})'")
    
    if not isfile (manifest_path):
        print (f"Manifest file {manifest_path} does not exist!")
        return (False, None, None, None, None)

    try:
        py_data_model_view = SchemaView (data_model_uri)
    except urllib.error.HTTPError as e:
        print (f"HTTP error {e.code} trying data model URI `{e.url}'")
        print ("If you've overridden the default using the `--data-model-uri' option, double-check that it's valid.")
        return (False, None, None, None, None)
            
    # Start by loading the manifest
    if (manifest_format == "ttl"):
        loader = RDFLibLoader ()
        dumper = YAMLDumper   ()
        try:
            manifest_obj = loader.load (source       = manifest_path
                                      , target_class = ManifestDesc
                                      , schemaview   = py_data_model_view)
            manifest_path_ttl = PurePath (manifest_path)
            (manifest_feasible, manifest_path_yaml) = convert_feasibility (
                input_path = manifest_path_ttl
              , target_ext = "yaml"
              , force      = force # More likely mistaken, don't forcibly overwrite!
            )
        except rdflib.plugins.parsers.notation3.BadSyntax:
            print (f"Cannot load file {manifest_path} with the RDF/TTL loader. Is your manifest a YAML manifest? (\"yaml\" `--serialisation' option)")
            return (False, None, None, None, None)

    elif (manifest_format == "yaml"):
        loader = YAMLLoader   ()
        dumper = RDFLibDumper ()

        try:
            manifest_obj = loader.load (source       = manifest_path
                                      , target_class = ManifestDesc)

            manifest_path_yaml = PurePath (manifest_path)
            (manifest_feasible, manifest_path_ttl) = convert_feasibility (
                input_path = manifest_path_yaml
              , target_ext = "ttl"
              , force      = True
            )
        except yaml.scanner.ScannerError:
            print (f"Cannot load file {manifest_path} with the YAML loader. Is your manifest an RDF/TTL manifest? (\"ttl\" `--serialisation' option)")
            return (False, None, None, None, None)
    else:
        print (f"Unrecognised serialisation mode {manifest_format}, cannot load extant object")
        return (False, None, None, None, None)
        
    manifest_uri = prefix_helper (py_data_model_view.schema,
                                  manifest_obj.atomic_name,
                                  prefixes["_base"])
    logging.debug(f"Manifest URI is {manifest_uri}")

    # Check early that the two operations are feasible (for returning if --dry-run)
    
    
    # Order slightly iffy, but schema conversion is not a prerequisite of the manifest conversion
    if (dry_run):
        for tab in manifest_obj.tables:
            fake_path_yaml             = f"{fake_cwd}{tab.schema_path_yaml}" 
            (schema_success, path_ttl) = coalesce_schema (fake_path_yaml, dry_run = dry_run, force = False, fake_cwd = fake_cwd)
            tab.schema_path_ttl        = path_ttl.name
            
        if (manifest_feasible):
            return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
        else:
            return (False, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
    else:
        for tab in manifest_obj.tables:
            # Start by subbing in successfully-serialised TTL schemata
            # Useful print statements in `coalesce_schema()' function
            fake_path_yaml             = f"{fake_cwd}{tab.schema_path_yaml}"
            (schema_success, path_ttl) = coalesce_schema (fake_path_yaml, dry_run = dry_run, force = force)
            if (schema_success):
                tab.schema_path_ttl = path_ttl.name

            # Now check hashes of resources
            table_uri      = tab.resource_path
            fake_table_uri = f"{fake_cwd}{table_uri}"
            print (f"Checking {fake_table_uri} ...")

            prereq_check = isfile (fake_table_uri)
        
            if (not prereq_check):
                raise ValueError (f"Error: target file {fake_table_uri} does not exist")
            else:
                with open (fake_table_uri, "rb") as fp:
                    data = fp.read ()
                    hash = sha384  (data)
                
                    if hash.hexdigest() != tab.resource_hash:
                        raise ValueError (f"{fake_table_uri} has changed, please revalidate with `fisdat'")
                
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
                dumper.dump (manifest_obj, manifest_path_yaml)
                return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
            else:
                dumper.dump (manifest_obj, manifest_path_ttl
                           , schemaview = py_data_model_view
                           , prefix_map = prefixes)
                return (True, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
            
        else:
            print ("Conversion of YAML manifest object to TTL is not feasible!")
            return (False, manifest_obj, manifest_path_yaml, manifest_path_ttl, manifest_uri)
    
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
               , "rap":   "https://marine.gov.scot/metadata/saved/rap/"
               , "saved": "https://marine.gov.scot/metadata/saved/schema/" }

    (test_manifest, manifest_obj, manifest_yaml, manifest_ttl, manifest_uri) = coalesce_manifest (
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
                      , manifest_uri       = manifest_uri
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
