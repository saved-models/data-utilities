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

from fisdat.utils import fst, extension_helper, job_table
from fisdat.ns import CSVW

## data read/write buffer size, 1MB
BUFSIZ=1048576

def upload_files (args  : [str]
                , files : [str]
                , owner : str
                , ts    : str) -> str:
    gen_path = lambda owner, ts, extra : owner + "/" + ts + "/" + extra
    client   = storage.Client()
    bucket   = client.bucket(args.bucket)
    jobuuid  = str(uuid.uuid1())
    path     = gen_path (owner, ts, args.directory) if args.directory is not None else gen_path (owner, ts, jobuuid)
    for fname in files:
        fpath = path + "/" + fname
        print (f"Uploading gs://{args.bucket}/{fpath} ...")
        start = time.time ()
        blob  = bucket.blob (fpath)
        blob.timeout=86400
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
    from google.cloud import client
    c = client.Client()
    return c._credentials.service_account_email

# Can't type this as we haven't compiled the model to Python data-classes yet
# Instead use the horrible target_class thing below
def load_manifest (data_model_path : PurePath, manifest_path : PurePath, verbose : bool = False):
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
    
    py_data_model_base   = PythonGenerator (data_model)
    py_data_model_module = py_data_model_base.compile_module ()
    py_data_model_view   = py_data_model_base.schemaview

    if (manifest_ext != "rdf" and verbose):
        print (f"Warning: target extension has a .{manifest_ext} extension, but will actually be serialised as RDF")

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
    parser = argparse.ArgumentParser("fisup")
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
        "-s", "--source", default=source(), help="Data source email"
    )
    parser.add_argument("manifest", help="Manifest file")

    args = parser.parse_args()

    if not isfile(args.manifest):
        raise ValueError(f"No such file: {args.manifest}")

    if args.unsecure:
        from rdflib import _networking
        from fisdat import kludge

        _networking._urlopen = kludge._urlopen

    data_model_path = PurePath ("data-model/src/model/meta.yaml")
    manifest_path   = PurePath (args.manifest)
    manifest_obj    = load_manifest (data_model_path, manifest_path, verbose = True)
        
    # Equivalent of dumping JSON in the old CLI:
    print (job_table (manifest_obj, preamble=False, mode='r'))

    manifest_obj.source = args.source
    
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

    data      = [table.data_uri    for table in manifest_obj.tables]
    schemas   = [table.data_schema for table in manifest_obj.tables]
    timestamp = datetime.today ().strftime ('%Y%m%d')
    shortname = manifest_obj.source.split('@')[0]     
    url       = upload_files (args, [basename (args.manifest)] + data + schemas, shortname, timestamp)

    print(f"Successfully uploaded your dataset to {url}")



def old_cli () -> None:
    """
    Command line interface
    """
    parser = argparse.ArgumentParser("fisup")
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
        "-s", "--source", default=source(), help="Data source email"
    )
    parser.add_argument("manifest", help="Manifest file")

    args = parser.parse_args()

    if not isfile(args.manifest):
        raise ValueError(f"No such file: {args.manifest}")
    
    if args.unsecure:
        from rdflib import _networking
        from fisdat import kludge

        _networking._urlopen = kludge._urlopen
        
    with open(args.manifest) as fp:
        manifest = json.load(fp)
        manifest["source"] = args.source
    with open(args.manifest, "w") as fp:
        json.dump(manifest, fp, indent=4)

    mdir = dirname(args.manifest)
    if mdir != "":
        chdir(mdir)

    for table in manifest.get("tables", []):
        print(f"Checking {table['url']} ...")
        with open(table["url"], "rb") as fp:
            data = fp.read()
            hash = sha384(data)
            if hash.hexdigest() != table["fileHash"]:
                raise ValueError(f"{table['url']} has changed, please revalidate with `fisdat'")

    data      = [table["url"] for table in manifest.get("tables", [])]
    schemas   = [table["tableSchema"] for table in manifest.get("tables", [])]
    timestamp = datetime.today().strftime('%Y%m%d')
    shortname = manifest["source"].split('@')[0]     
    url       = upload_files(args, [basename(args.manifest)] + data + schemas, shortname, timestamp)

    print(f"Successfully uploaded your dataset to {url}")
    
