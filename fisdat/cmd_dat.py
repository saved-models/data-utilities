from rdflib import Graph, Namespace, Literal
from rdflib.collection import Collection
from rdflib.term import XSDToPython
from csvwlib.utils.TypeConverter import TypeConverter
import csv
import argparse
from os.path import isfile
import json

CSVW = Namespace("http://www.w3.org/ns/csvw#")

def fst(g):
  """
  Utility function. RDFLib gives generators all over the place and
  usually we just want a single value.
  """
  for e in g:
    return e
  raise Exception("Generator is empty")

def complex_datatype(schema, dt):
  """
  CSVW has validation rules for things like dates. Use the library
  to do the validation rather than implementing it ourselves.
  """
  dtdict = { p.removeprefix(CSVW): o
             for _,p,o in schema.triples((dt, None, None)) }
  convert = lambda x: TypeConverter.convert(x, dtdict)
  return convert

def columns(schema):
  """
  Get a list of (column_name, datatype_validator) out of the schema.
  """
  column_ids = list(schema.objects(None, CSVW["column"]))
  if len(column_ids) == 0:
    raise Exception("Schema has no columns")
  if len(column_ids) > 1:
    raise Exception("Schema has too many columns")
  columns = column_ids.pop()
  for c in Collection(schema, columns):
    name   = fst(schema.objects(c, CSVW["name"]))
    xdtype = fst(schema.objects(c, CSVW["datatype"]))
    dtype  = XSDToPython[xdtype] if xdtype in XSDToPython else complex_datatype(schema, xdtype)
    if dtype is None:
      dtype = str
    yield name, dtype

def cli():
  """
  Command line interface
  """
  parser = argparse.ArgumentParser("fisdat")
  parser.add_argument("-s", "--strict", action="store_true", help="Strict validation")
  parser.add_argument("schema", help="Schema file/URI")
  parser.add_argument("csvfile", help="CSV data file")
  parser.add_argument("manifest", help="Manifest file")

  args = parser.parse_args()

  def error(s):
    """
    Raise an exception if we are in strict mode, otherwise just print
    things like validation errors.
    """
    if args.strict:
      raise Exception(s)
    else:
      print(s)

  # Load the schema      
  schema = Graph().parse(location=args.schema, format="json-ld")
  # print(schema.serialize(format="n3"))

  # Get the columns out of it  
  cols = list(columns(schema))

  # Read the CSV file  
  seen_header = False
  with open(args.csvfile) as fp:
    for i, row in enumerate(csv.reader(fp)):
      # Check that the header is present and has the right columns      
      if not seen_header:
        lhead = len(row)
        lschema = len(cols)
        if lhead != lschema:
          error(f"CSV header and schema have different numbers of columns: {lhead} vs {lschema}")
        for h, c in zip(row, cols):
          name, _ = c
          if str(name) != h:
            error(f"Mismatch of column name: {name} in schema {h} in data file")
        seen_header = True
        continue
      else:
        # For each row, check that each column has the right kind of data
        for r, c in zip(row, cols):
          col, validator = c
          try:
            validator(r)
          except Exception as e:
            error(f"Row {i} value {r} in column {col} does not validate")

  ## Add the CSV file and its schema to the manifest      
  manifest = {
    "@context": str(CSVW)[:-1],
    "tables": []
  }
  
  # Read the manifest if it exists
  if isfile(args.manifest):
    with open(args.manifest) as fp:
      manifest.update(json.load(fp))

  # Keep any other tables already present      
  manifest["tables"] = [t for t in manifest["tables"]
                        if t.get("url") != args.csvfile]
  # Add this table and its schema to the manifest
  manifest["tables"].append({ "url": args.csvfile, "tableSchema": args.schema })

  # Save the new manifest
  with open(args.manifest, "w+") as fp:
    json.dump(manifest, fp, indent=4)
