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
import logging
import time
import uuid

from linkml.generators.pythongen     import PythonGenerator
from linkml.utils.schema_builder     import SchemaBuilder
from linkml.validator                import validate_file
from linkml.validator.report         import Severity, ValidationResult, ValidationReport
from linkml_runtime.loaders          import RDFLibLoader
from linkml_runtime.utils.schemaview import SchemaView, SchemaDefinition

from fisdat import __version__, __commit__
from fisdat.utils import fst, extension_helper, job_table
from fisdat.ns import CSVW
from importlib import resources as ir
from . import data_model as dm

## data read/write buffer size, 1MB
BUFSIZ=1048576

def upload_files (args      : [str]
                , files     : [str]
                , owner     : str
                , ts        : str) -> str:
    logging.debug (f"Called `upload_files (args = {args}, files = {files}, owner = {owner}, ts = {ts})'")

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

def source () -> str:
    logging.debug ("Called `source()'")
    from google.cloud import client
    c   = client.Client ()
    res = c._credentials.service_account_email
    logging.info (f"GCP account e-mail: {res}")
    return res

# Can't type this as we haven't compiled the model to Python data-classes yet
# Instead use the horrible target_class thing below
def load_manifest (data_model_path : PurePath, manifest_path : PurePath):
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

    logging.debug (f"Called `load_manifest (data_model_path = {data_model}, manifest_path = {manifest})'")
    
    py_data_model_base   = PythonGenerator (data_model)
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview
    
    if (manifest_ext != "rdf"):
        logging.info (f"Warning: target extension has a .{manifest_ext} extension, but will actually be serialised as RDF/TTL")

    target_class      = py_data_model_module.__dict__["ManifestDesc"]
    loader            = RDFLibLoader ()
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
    parser.add_argument ("--data-model"
                       , help     = "Data model YAML specification in fisdat/data_model/src/model"
                       , default  = "meta")
    parser.add_argument ("-n", "--no-upload", "--dry-run"
                       , help     = "Don't upload files"
                       , action   = "store_true")
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
    
    # Need this to pass verbosity= into source()
    if (args.source is None):
        data_source_email = source ()
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
    manifest_obj    = load_manifest (data_model_path, manifest_path)
        
    # Equivalent of dumping JSON in the old CLI:
    print (job_table (manifest_obj, preamble=False, mode='r'))

    manifest_obj.source = data_source_email
    
    mdir = dirname (args.manifest)
    if (mdir != ""):
        chdir (mdir)

    for table in manifest_obj.tables:
        target_uri = table.path
        print (f"Checking {target_uri} ...")

        prereq_check = isfile (target_uri)
        
        if (not prereq_check):
            print (f"Error: target file {target_uri} does not exist")
        else:
            with open (target_uri, "rb") as fp:
                data = fp.read ()
                hash = sha384  (data)
                
                if hash.hexdigest() != table.hash:
                    raise ValueError(f"{target_uri} has changed, please revalidate with `fisdat'")        

    data       = [table.path    for table in manifest_obj.tables]
    schemas    = [table.schema_path for table in manifest_obj.tables]
    time_stamp = datetime.today ().strftime ('%Y%m%d')
    short_name = manifest_obj.source.split('@')[0]     
    url        = upload_files (args
                            , [basename (args.manifest)] + data + schemas
                            , short_name, time_stamp)

    if (not args.no_upload):
        print(f"Successfully uploaded your data-set to {url}")
    else:
        print(f"Would have uploaded your data-set to {url}")
