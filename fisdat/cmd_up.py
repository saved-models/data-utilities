from rdflib import Graph, Namespace, Literal
from rdflib.collection import Collection
from google.cloud import storage
from fisdat.utils import fst
from fisdat.ns import CSVW
from hashlib import sha384
import argparse
from os.path import isfile, basename, dirname
from os import chdir
import uuid
import json

def upload_files(args, files):
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    path = args.directory if args.directory is not None else str(uuid.uuid1())
    for fname in files:
        fpath = path + "/" + fname
        print(f"Uploading gs://{args.bucket}/{fpath} ...")
        blob = bucket.blob(fpath)
        with open(fname, "r") as fp:
            with blob.open("w") as bp:
                bp.write(fp.read())
    return f"gs://{args.bucket}/{path}"

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

    url = upload_files(args, [basename(args.manifest)] + [table["url"] for table in manifest.get("tables", [])])
    print(f"Successfully uploaded your dataset to {url}")
    
        
