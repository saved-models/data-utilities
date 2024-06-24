from fisdat.utils import extension_helper, prefix_helper, validation_helper

from linkml_runtime.linkml_model import SchemaDefinition

import logging
from pathlib import PurePath
import os
import unittest

'''
Note here that the tests often end in a `try'/`exception' block for `os.remove'.
This is a succint, if rough way of checking that the file exists as expected.
'''

logging_format = "%(levelname)s [%(asctime)s] [`%(filename)s\' `%(funcName)s\' (l.%(lineno)d)] ``%(message)s\'\'"
logging_level  = logging.DEBUG
        
data_model_uri    = "https://marine.gov.scot/metadata/saved/schema/meta.yaml"
data_model_uri_ne = "https://marine.gov.scot/metadata/saved/schema/.test.yaml"

prefixes          = { "_base": "https://marine.gov.scot/metadata/saved/rap/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt      = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
prefixes_alt_alt  = { "_base": "https://marine.gov.scot/metadata/saved/rap_alt_alt/"
                    , "saved": "https://marine.gov.scot/metadata/saved/schema/" }

manifest_yaml = "/tmp/manifest.yaml"
manifest_ttl  = "/tmp/manifest.ttl"
manifest_name = "RootManifest"

data0    = "examples/sentinel_cages/sentinel_cages_cleaned.csv"
data1    = "examples/sentinel_cages/Sentinel_cage_station_info_6.csv"
data_ne  = "examples/sentinel_cages/.cagedata.csv"
data_bad = "examples/sentinel_cages/Sentinel_cage_sampling_info_update_01122022.csv"

schema0   = "examples/sentinel_cages/sentinel_cages_sampling.yaml"
schema1   = "examples/sentinel_cages/sentinel_cages_site.yaml"
schema_ne = "examples/sentinel_cages/.sampling.yaml"

schema_definition0 = SchemaDefinition (
    name           = "KnownGoodSchemaDefinition"
  , id             = "known_good_schema_definition"
  , prefixes       = { "rap"  : "https://marine.gov.scot/metadata/saved/rap/"
                     , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
  , default_prefix = "rap"
)
schema_definition1 = SchemaDefinition (
    name           = "BadPrefixMappings"
  , id             = "bad_prefixes_schema_definition"
  , prefixes       = None
  , default_prefix = "rap"
)
schema_definition2 = SchemaDefinition (
    name           = "BadDefaultPrefix0"
  , id             = "bad_default_prefix_schema_definition0"
  , prefixes       = { "rap"  : "https://marine.gov.scot/metadata/saved/rap/"
                     , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
  , default_prefix = "rap" # default prefix does exist
)
schema_definition3 = SchemaDefinition (
    name           = "BadDefaultPrefix1"
  , id             = "bad_default_prefix_schema_definition1"
  , prefixes       = { "rap"  : "https://marine.gov.scot/metadata/saved/rap/"
                     , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
  , default_prefix = "curr" # default prefix specified but not in lookup table
)
schema_definition4 = SchemaDefinition (
    name           = "BadDefaultPrefix2"
  , id             = "bad_default_prefix_schema_definition2"
  , prefixes       = { "rap"  : "https://marine.gov.scot/metadata/saved/rap/"
                     , "saved": "https://marine.gov.scot/metadata/saved/schema/" }
  , default_prefix = None # no default prefix, use fallback
)

class TestMisc (unittest.TestCase):
    '''
    Validation test case 1: Known-good data                  -> True
    Validation test case 2: Mismatched schema file           -> False
    Validation test case 3: Non-existent data / schema files -> False
    Validation test case 4: Invalid target class             -> False

    Note for target class, this is an actual class object. The Python
    interpreter will anyway error if we provide a non-existent class,
    as opposed to one which is extant but incorrect.
    '''
    def test_validate0 (self):
        print ("Validation test case 1: Known-good data")

        test = validation_helper (
            data         = data0
          , schema       = schema0
          , target_class = "TableSchema"
        )
        self.assertTrue (test)

    def test_validate1 (self):
        print ("Validation test case 2: Mismatched schema file")

        test = validation_helper (
            data         = data0
          , schema       = schema1
          , target_class = "TableSchema"
        )
        self.assertFalse (test)

    def test_validate2 (self):
        print ("Validation test case 3: Non-existent data / schema files")

        test0 = validation_helper (
            data         = data0
          , schema       = schema_ne
          , target_class = "TableSchema"
        )
        test1 = validation_helper (
            data         = data_ne
          , schema       = schema0
          , target_class = "TableSchema"
        )
        self.assertFalse (test0 or test1)

    def test_validate3 (self):
        print ("Validation test case 4: Invalid target class")

        test = validation_helper (
            data         = data0
          , schema       = schema0
          , target_class = "TableMiscellanea"
        )
        self.assertFalse (test)
            
    '''
    Extension helper case 1: "/etc/netstart.sh" -> "sh"
    Extension helper case 2: "/etc/netstart"    -> ""
    Extension helper case 3: "test.rdf"         -> "rdf"
    Extension helper case 4: ""                 -> ""
    '''
    def test_extensions0 (self):
        print ("Extension helper case 1: \"/etc/netstart.sh\"")
        self.assertTrue (extension_helper (PurePath ("/etc/netstart.sh")) == "sh")
    def test_extensions1 (self):
        print ("Extension helper case 2: \"/etc/netstart\"")
        self.assertTrue (extension_helper (PurePath ("/etc/netstart")) == "")
    def test_extensions2 (self):
        print ("Extension helper case 3: \"test.rdf\"")
        self.assertTrue (extension_helper (PurePath ("test.rdf")) == "rdf")
    def test_extensions3 (self):
        print ("Extension helper case 4: \"\"")
        self.assertTrue (extension_helper (PurePath ("")) == "")

    '''
    1. If the prefix code/URI mappings are None, use the fallback prefix as we can't expand the default prefix code anyway
    2. If the prefix code/URI mappings are not None, try looking up the prefix matched, and expand or use fallback prefix
    3. If the prefix matched does not exist, try looking up the default prefix  and expand or use fallback prefixzip
    4. If the CURIE is not actually valid, concatenate the CURIE to the fallback prefix
    
    Prefix helper case 1 -> "https://marine.gov.scot/metadata/saved/schema/LeafManifest"
      - URI/CURIE: saved:LeafManifest
      - prefixes:  saved: ..., @base: ...
      - fallback:  rap_alt: ...
    Prefix helper case 2 -> "https://marine.gov.scot/metadata/saved/rap/LeafManifest"
      As above, but reference non-existent prefix "curr"
    Prefix helper case 3 -> "https://marine.gov.scot/metadata/saved/rap_alt/LeafManifest"
      As above, but no prefixes declared
    '''
    def test_prefixes0 (self):
        print ("Prefix helper case 1: Known-good test data")
        test = prefix_helper (
            schema_definition = schema_definition0
          , curie             = "saved:LeafManifest0"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        self.assertTrue (test == "https://marine.gov.scot/metadata/saved/schema/LeafManifest0")
        
    def test_prefixes1 (self):
        print ("Prefix helper case 2: Prefix code/URI mappings are None, use fallback URI")
        test = prefix_helper (
            schema_definition = schema_definition1
          , curie             = "rap:LeafManifest1"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        self.assertTrue (test == "https://marine.gov.scot/metadata/saved/rap_alt/LeafManifest1")

    def test_prefixes2 (self):
        print ("Prefix helper test case 3: Try matching default prefix")
        test0 = prefix_helper (
            schema_definition = schema_definition2
          , curie             = "curr:LeafManifest2"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        test1 = prefix_helper (
            schema_definition = schema_definition3
          , curie             = "curr:LeafManifest3"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        test2 = prefix_helper (
            schema_definition = schema_definition4
          , curie             = "curr:LeafManifest4"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        res0 = "https://marine.gov.scot/metadata/saved/rap/LeafManifest2"
        res1 = "https://marine.gov.scot/metadata/saved/rap_alt/LeafManifest3"
        res2 = "https://marine.gov.scot/metadata/saved/rap_alt/LeafManifest4"
        self.assertTrue (test0 == res0 and test1 == res1 and test2 == res2)

    def test_prefixes3 (self):
        print ("Prefix helper test case 4: Malformed CURIE")

        test0 = prefix_helper (
            schema_definition = schema_definition0
          , curie             = "curr_root_manifest"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt/"
        )
        test1 = prefix_helper (
            schema_definition = schema_definition0
          , curie             = "localhost:some:where:place"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt_alt/"
        )
        test2 = prefix_helper (
            schema_definition = schema_definition0
          , curie             = "http://localhost/scratch"
          , fallback_uri      = "https://marine.gov.scot/metadata/saved/rap_alt_alt_alt/"
        )
        res0 = "https://marine.gov.scot/metadata/saved/rap_alt/curr_root_manifest"
        res1 = "https://marine.gov.scot/metadata/saved/rap_alt_alt/localhost:some:where:place"
        res2 = "https://marine.gov.scot/metadata/saved/rap_alt_alt_alt/http://localhost/scratch"
        self.assertTrue (test0 == res0 and test1 == res1 and test2 == res2)
