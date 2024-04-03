from rdflib import Graph, Namespace, Literal
from rdflib.collection import Collection
from datetime import datetime
from google.cloud import storage
from google.cloud import client as gc
from hashlib import sha384
import argparse
from os.path import isfile, basename, dirname
from os import chdir
from pathlib import Path, PurePath
import json
import time
import uuid

from linkml.generators.pythongen     import PythonGenerator
from linkml.utils.datautils          import _get_context, _get_format, get_dumper, get_loader
from linkml.utils.schema_builder     import SchemaBuilder
from linkml.validator                import validate_file
from linkml.validator.report         import Severity, ValidationResult, ValidationReport
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

from fisdat import __version__, __commit__
from fisdat.utils import fst, extension_helper, job_table, vprint, vvprint
from fisdat.ns import CSVW
from importlib import resources as ir
from . import data_model as dm

## data read/write buffer size, 1MB
BUFSIZ=1048576

def upload_files (args      : [str]
                , files     : [str]
                , owner     : str
                , ts        : str
                , verbosity : int) -> str:
    vvprint (f"Called `upload_files (args = {args}, files = {files}, owner = {owner}, ts = {ts})'", verbosity)

    gen_path = lambda owner, ts, extra : owner + "/" + ts + "/" + extra
    client   = storage.Client()
    bucket   = client.bucket(args.bucket)
    jobuuid  = str(uuid.uuid1())
    path     = gen_path (owner, ts, args.directory) if args.directory is not None else gen_path (owner, ts, jobuuid)
    if (not args.no_upload):
        upload_message = "Uploading to"
    else:
        upload_message = "Would upload to"
    
    for fname in files:
        fpath = path + "/" + fname
        print (f"{upload_message}: gs://{args.bucket}/{fpath} ...")
        start = time.time ()
        blob  = bucket.blob (fpath)
        blob.timeout=86400
        if (not args.no_upload):
            with open(fname, "rb") as fp:
                with blob.open("wb") as bp:
                    while True:
                        stuff = fp.read(BUFSIZ)
                        if len(stuff) == 0:
                            break
                        bp.write(stuff)
                        end = time.time ()
                        abs_time = end - start
            if (abs_time < 1):
                elapsed = round (abs_time, 2)
            else:
                elapsed = round (abs_time)
            print (f"Uploaded {fname} in {elapsed}s")
    return f"gs://{args.bucket}/{path}"

def source (verbosity : int) -> str:
    vvprint ("Called `source()'", verbosity)
    from google.cloud import client
    c   = client.Client ()
    res = c._credentials.service_account_email
    vprint (f"GCP account e-mail: {res}", verbosity)
    return res

# Can't type this as we haven't compiled the model to Python data-classes yet
# Instead use the horrible target_class thing below
def load_manifest (data_model_path : PurePath, manifest_path : PurePath, verbosity : int):
    '''
    Note that this duplicates some of the code in cmd_dat.py, since
    loading the schema is a part of the `append_manifest()' function.

    The data-model provides a JSON-LD context and/or a `SchemaView'
    object, which is necessary for serialising JSON-LD and RDF/TTL.
    
    The problem is that that function needs py_data_model_module,
    as well. The way to fix this is to make use of the Python
    data-model as a class, which will enable us to avoid serialising
    the py_data_model &c.
    '''
    data_model   = str (data_model_path)
    manifest     = str (manifest_path)
    manifest_ext = extension_helper (manifest_path)

    vvprint (f"Called `load_manifest (data_model_path = {data_model}, manifest_path = {manifest})'", verbosity)
    
    py_data_model_base   = PythonGenerator (data_model)
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview
    
    if (manifest_ext != "rdf"):
        vprint (f"Warning: target extension has a .{manifest_ext} extension, but will actually be serialised as RDF/TTL", verbosity)

    target_class      = py_data_model_module.__dict__["JobDesc"]
    loader            = get_loader  ("rdf")
    original_manifest = loader.load (source       = manifest
                                   , target_class = target_class
                                   , schemaview   = py_data_model_view)
    return (original_manifest)

def cli () -> None:
    """
    Command line interface
    """
    print (f"This is fisdat version {__version__}, commit {__commit__}")
    
    parser = argparse.ArgumentParser("fisup")
    verbgr = parser.add_mutually_exclusive_group (required = False)

    parser.add_argument(
        "-u", "--unsecure", action="store_true", help="Disable SSL validation"
    )
    parser.add_argument(
        "-b", "--bucket", default="saved-fisdat", help="Bucket to upload into"
    )
    parser.add_argument(
        "-d", "--directory", default=None, help="Directory within bucket to upload into"
    )
    parser.add_argument(
        "-s", "--source", default=None, help="Data source email"
    )
    parser.add_argument(
        "manifest", help="Manifest file"
    )
    parser.add_argument ("--data-model"
                       , help    = "Data model YAML specification"
                       , default = "meta")
    parser.add_argument ("-n", "--no-upload", "--dry-run"
                       , help   = "Don't upload files"
                       , action = "store_true")
    verbgr.add_argument (
        "-v", "--verbose", required = False, action = "store_true"
      , help = "Show more information about current running state"
    )
    verbgr.add_argument (
        "-vv", "--extra-verbose", required = False, action = "store_true"
      , help = "Show even more information about current running state"
    )

    args = parser.parse_args ()
    
    if (args.verbose):
        verbosity = 1
    elif (args.extra_verbose):
        verbosity = 2
    else:
        verbosity = 0
    # Need this to pass verbosity= into source()
    if (args.source is None):
        data_source_email = source (verbosity)
    else:
        data_source_email = args.source

    if not isfile(args.manifest):
        raise ValueError(f"No such file: {args.manifest}")

    if args.unsecure:
        from rdflib import _networking
        from fisdat import kludge

        _networking._urlopen = kludge._urlopen

    data_model_path = ir.files (dm) / f"src/model/{args.data_model}.yaml"
    manifest_path   = PurePath (args.manifest)
    manifest_obj    = load_manifest (data_model_path, manifest_path, verbosity)
        
    # Equivalent of dumping JSON in the old CLI:
    print (job_table (manifest_obj, preamble=False, mode='r'))

    manifest_obj.source = data_source_email
    
    mdir = dirname (args.manifest)
    if (mdir != ""):
        chdir (mdir)

    for table in manifest_obj.tables:
        target_uri = table.data_uri
        print (f"Checking {target_uri} ...")

        prereq_check = isfile (target_uri)
        
        if (not prereq_check):
            print (f"Error: target file {target_uri} does not exist")
        else:
            with open (target_uri, "rb") as fp:
                data = fp.read ()
                hash = sha384  (data)
                
                if hash.hexdigest() != table.data_hash:
                    raise ValueError(f"{target_uri} has changed, please revalidate with `fisdat'")        

    data       = [table.data_uri    for table in manifest_obj.tables]
    schemas    = [table.data_schema for table in manifest_obj.tables]
    time_stamp = datetime.today ().strftime ('%Y%m%d')
    short_name = manifest_obj.source.split('@')[0]     
    url        = upload_files (args
                            , [basename (args.manifest)] + data + schemas
                            , short_name, time_stamp, verbosity)

    if (not args.no_upload):
        print(f"Successfully uploaded your data-set to {url}")
    else:
        print(f"Would have uploaded your data-set to {url}")
