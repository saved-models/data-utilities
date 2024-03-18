from rdflib import Graph, Namespace, Literal
from rdflib.collection import Collection
from datetime import datetime
from google.cloud import storage
from google.cloud import client as gc
from fisdat.utils import fst
from fisdat.ns import CSVW
from hashlib import sha384
import argparse
from os.path import isfile, basename, dirname
from os import chdir
import uuid
import json

## Set this for GCP uploads, and use blob.upload_from_filename directly
## Default value is 100MB, time-out is 1 minute => requires 13.67 Mb/s
## before the time-out…?
## https://github.com/googleapis/python-storage/issues/74
## I've set this both here and on the blob storage.blob() instance,
## and set the command to upload_from_filename.
## Seems to work on my end…
## data read/write buffer size, 1MB
BUFSIZ                           = 1048576
storage.blob._DEFAULT_CHUNKSIZE  = BUFSIZ
storage.blob._MAX_MULTIPART_SIZE = BUFSIZ

def upload_files(args, files, owner, ts):
    gen_path = lambda owner, ts, extra : owner + "/" + ts + "/" + extra
    client   = storage.Client()
    bucket   = client.bucket(args.bucket)
    jobuuid  = str(uuid.uuid1())
    path     = gen_path (owner, ts, args.directory) if args.directory is not None else gen_path (owner, ts, jobuuid)
    for fname in files:
        fpath = path + "/" + fname
        print(f"Uploading gs://{args.bucket}/{fpath} ...")
        blob = bucket.blob(fpath)
        blob.chunk_size = BUFSIZ
        blob.upload_from_filename(fname)
    return f"gs://{args.bucket}/{path}"

def source():
    from google.cloud import client
    c = client.Client()
    return c._credentials.service_account_email

def cli():
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
    
