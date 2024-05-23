# Auto generated from meta.yaml by pythongen.py version: 0.0.1
# Generation date: 2024-05-23T13:29:03
# Schema: meta
#
# id: https://marine.gov.scot/metadata/saved/schema/meta/
# description:
# license: https://creativecommons.org/publicdomain/zero/1.0/

import dataclasses
import re
from jsonasobj2 import JsonObj, as_dict
from typing import Optional, List, Union, Dict, ClassVar, Any
from dataclasses import dataclass
from datetime import date, datetime
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, PvFormulaOptions

from linkml_runtime.utils.slot import Slot
from linkml_runtime.utils.metamodelcore import empty_list, empty_dict, bnode
from linkml_runtime.utils.yamlutils import YAMLRoot, extended_str, extended_float, extended_int
from linkml_runtime.utils.dataclass_extensions_376 import dataclasses_init_fn_with_kwargs
from linkml_runtime.utils.formatutils import camelcase, underscore, sfx
from linkml_runtime.utils.enumerations import EnumDefinitionImpl
from rdflib import Namespace, URIRef
from linkml_runtime.utils.curienamespace import CurieNamespace
from linkml_runtime.linkml_model.types import Float, Integer, String, Uri
from linkml_runtime.utils.metamodelcore import URI

metamodel_version = "1.7.0"
version = None

# Overwrite dataclasses _init_fn to add **kwargs in __init__
dataclasses._init_fn = dataclasses_init_fn_with_kwargs

# Namespaces
DC = CurieNamespace('dc', 'http://purl.org/dc/elements/1.1/')
DCTERMS = CurieNamespace('dcterms', 'http://purl.org/dc/terms/')
JOB = CurieNamespace('job', 'https://marine.gov.scot/metadata/saved/schema/job_')
LINKML = CurieNamespace('linkml', 'https://w3id.org/linkml/')
PAV = CurieNamespace('pav', 'http://purl.org/pav/')
RAP = CurieNamespace('rap', 'https://marine.gov.scot/metadata/saved/rap/')
SAVED = CurieNamespace('saved', 'https://marine.gov.scot/metadata/saved/schema/')
DEFAULT_ = SAVED


# Types
class LatLonType(str):
    type_class_uri = SAVED["LatLon"]
    type_class_curie = "saved:LatLon"
    type_name = "LatLonType"
    type_model_uri = SAVED.LatLonType


# Class references
class TableDescAtomicName(extended_str):
    pass


class ExtColumnDescAtomicName(extended_str):
    pass


class JobDescAtomicName(extended_str):
    pass


class ManifestDescAtomicName(extended_str):
    pass


@dataclass
class TableDesc(YAMLRoot):
    """
    Manifest: data source descriptive attributes
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SAVED["TableDesc"]
    class_class_curie: ClassVar[str] = "saved:TableDesc"
    class_name: ClassVar[str] = "TableDesc"
    class_model_uri: ClassVar[URIRef] = SAVED.TableDesc

    atomic_name: Union[str, TableDescAtomicName] = None
    resource_path: Union[str, URI] = None
    resource_hash: str = None
    schema_path_yaml: Union[str, URI] = None
    title: Optional[str] = None
    description: Optional[str] = None
    schema_path_ttl: Optional[Union[str, URI]] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.atomic_name):
            self.MissingRequiredField("atomic_name")
        if not isinstance(self.atomic_name, TableDescAtomicName):
            self.atomic_name = TableDescAtomicName(self.atomic_name)

        if self._is_empty(self.resource_path):
            self.MissingRequiredField("resource_path")
        if not isinstance(self.resource_path, URI):
            self.resource_path = URI(self.resource_path)

        if self._is_empty(self.resource_hash):
            self.MissingRequiredField("resource_hash")
        if not isinstance(self.resource_hash, str):
            self.resource_hash = str(self.resource_hash)

        if self._is_empty(self.schema_path_yaml):
            self.MissingRequiredField("schema_path_yaml")
        if not isinstance(self.schema_path_yaml, URI):
            self.schema_path_yaml = URI(self.schema_path_yaml)

        if self.title is not None and not isinstance(self.title, str):
            self.title = str(self.title)

        if self.description is not None and not isinstance(self.description, str):
            self.description = str(self.description)

        if self.schema_path_ttl is not None and not isinstance(self.schema_path_ttl, URI):
            self.schema_path_ttl = URI(self.schema_path_ttl)

        super().__post_init__(**kwargs)


@dataclass
class ScopeDesc(YAMLRoot):
    """
    Manifest: column scope description and attributes including variable underpinning the column
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SAVED["ScopeDesc"]
    class_class_curie: ClassVar[str] = "saved:ScopeDesc"
    class_name: ClassVar[str] = "ScopeDesc"
    class_model_uri: ClassVar[URIRef] = SAVED.ScopeDesc

    column: str = None
    variable: Union[str, ExtColumnDescAtomicName] = None
    table: Union[str, TableDescAtomicName] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.column):
            self.MissingRequiredField("column")
        if not isinstance(self.column, str):
            self.column = str(self.column)

        if self._is_empty(self.variable):
            self.MissingRequiredField("variable")
        if not isinstance(self.variable, ExtColumnDescAtomicName):
            self.variable = ExtColumnDescAtomicName(self.variable)

        if self._is_empty(self.table):
            self.MissingRequiredField("table")
        if not isinstance(self.table, TableDescAtomicName):
            self.table = TableDescAtomicName(self.table)

        super().__post_init__(**kwargs)


@dataclass
class ExtColumnDesc(YAMLRoot):
    """
    Catch-all external column description
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SAVED["ExtColumnDesc"]
    class_class_curie: ClassVar[str] = "saved:ExtColumnDesc"
    class_name: ClassVar[str] = "ExtColumnDesc"
    class_model_uri: ClassVar[URIRef] = SAVED.ExtColumnDesc

    atomic_name: Union[str, ExtColumnDescAtomicName] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.atomic_name):
            self.MissingRequiredField("atomic_name")
        if not isinstance(self.atomic_name, ExtColumnDescAtomicName):
            self.atomic_name = ExtColumnDescAtomicName(self.atomic_name)

        super().__post_init__(**kwargs)


@dataclass
class JobDesc(YAMLRoot):
    """
    Manifest: job specification attributes which specific job sub-classes inherit
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SAVED["JobDesc"]
    class_class_curie: ClassVar[str] = "saved:JobDesc"
    class_name: ClassVar[str] = "JobDesc"
    class_model_uri: ClassVar[URIRef] = SAVED.JobDesc

    atomic_name: Union[str, JobDescAtomicName] = None
    job_type: Union[str, "JobType"] = None
    title: Optional[str] = None
    job_scope_descriptive: Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]] = empty_list()
    job_scope_collected: Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]] = empty_list()
    job_scope_modelled: Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]] = empty_list()

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.atomic_name):
            self.MissingRequiredField("atomic_name")
        if not isinstance(self.atomic_name, JobDescAtomicName):
            self.atomic_name = JobDescAtomicName(self.atomic_name)

        if self._is_empty(self.job_type):
            self.MissingRequiredField("job_type")
        if not isinstance(self.job_type, JobType):
            self.job_type = JobType(self.job_type)

        if self.title is not None and not isinstance(self.title, str):
            self.title = str(self.title)

        if not isinstance(self.job_scope_descriptive, list):
            self.job_scope_descriptive = [self.job_scope_descriptive] if self.job_scope_descriptive is not None else []
        self.job_scope_descriptive = [v if isinstance(v, ScopeDesc) else ScopeDesc(**as_dict(v)) for v in self.job_scope_descriptive]

        if not isinstance(self.job_scope_collected, list):
            self.job_scope_collected = [self.job_scope_collected] if self.job_scope_collected is not None else []
        self.job_scope_collected = [v if isinstance(v, ScopeDesc) else ScopeDesc(**as_dict(v)) for v in self.job_scope_collected]

        if not isinstance(self.job_scope_modelled, list):
            self.job_scope_modelled = [self.job_scope_modelled] if self.job_scope_modelled is not None else []
        self.job_scope_modelled = [v if isinstance(v, ScopeDesc) else ScopeDesc(**as_dict(v)) for v in self.job_scope_modelled]

        super().__post_init__(**kwargs)


@dataclass
class ManifestDesc(YAMLRoot):
    """
    Manifest: job invocation specification
    """
    _inherited_slots: ClassVar[List[str]] = []

    class_class_uri: ClassVar[URIRef] = SAVED["ManifestDesc"]
    class_class_curie: ClassVar[str] = "saved:ManifestDesc"
    class_name: ClassVar[str] = "ManifestDesc"
    class_model_uri: ClassVar[URIRef] = SAVED.ManifestDesc

    atomic_name: Union[str, ManifestDescAtomicName] = None
    tables: Union[Dict[Union[str, TableDescAtomicName], Union[dict, TableDesc]], List[Union[dict, TableDesc]]] = empty_dict()
    jobs: Union[Dict[Union[str, JobDescAtomicName], Union[dict, JobDesc]], List[Union[dict, JobDesc]]] = empty_dict()
    gcp_source: Optional[str] = None
    local_version: Optional[str] = None

    def __post_init__(self, *_: List[str], **kwargs: Dict[str, Any]):
        if self._is_empty(self.atomic_name):
            self.MissingRequiredField("atomic_name")
        if not isinstance(self.atomic_name, ManifestDescAtomicName):
            self.atomic_name = ManifestDescAtomicName(self.atomic_name)

        if self._is_empty(self.tables):
            self.MissingRequiredField("tables")
        self._normalize_inlined_as_list(slot_name="tables", slot_type=TableDesc, key_name="atomic_name", keyed=True)

        if self._is_empty(self.jobs):
            self.MissingRequiredField("jobs")
        self._normalize_inlined_as_list(slot_name="jobs", slot_type=JobDesc, key_name="atomic_name", keyed=True)

        if self.gcp_source is not None and not isinstance(self.gcp_source, str):
            self.gcp_source = str(self.gcp_source)

        if self.local_version is not None and not isinstance(self.local_version, str):
            self.local_version = str(self.local_version)

        super().__post_init__(**kwargs)


# Enumerations
class JobType(EnumDefinitionImpl):

    ignore = PermissibleValue(
        text="ignore",
        description="""Dummy job to ignore: Associated job description may describe arbitrary columns, underlying variables, or tables""")
    density = PermissibleValue(
        text="density",
        description="ODE version of the sea lice accumulation model")

    _defn = EnumDefinition(
        name="JobType",
    )

# Slots
class slots:
    pass

slots.column_modelled = Slot(uri=SAVED.column_modelled, name="column_modelled", curie=SAVED.curie('column_modelled'),
                   model_uri=SAVED.column_modelled, domain=None, range=Optional[str])

slots.column_collected = Slot(uri=SAVED.column_collected, name="column_collected", curie=SAVED.curie('column_collected'),
                   model_uri=SAVED.column_collected, domain=None, range=Optional[str])

slots.column_descriptive = Slot(uri=SAVED.column_descriptive, name="column_descriptive", curie=SAVED.curie('column_descriptive'),
                   model_uri=SAVED.column_descriptive, domain=None, range=Optional[str])

slots.title = Slot(uri=DCTERMS.title, name="title", curie=DCTERMS.curie('title'),
                   model_uri=SAVED.title, domain=None, range=Optional[str])

slots.description = Slot(uri=DCTERMS.description, name="description", curie=DCTERMS.curie('description'),
                   model_uri=SAVED.description, domain=None, range=Optional[str])

slots.type = Slot(uri=DCTERMS.type, name="type", curie=DCTERMS.curie('type'),
                   model_uri=SAVED.type, domain=None, range=Optional[str])

slots.provenance = Slot(uri=SAVED.provenance, name="provenance", curie=SAVED.curie('provenance'),
                   model_uri=SAVED.provenance, domain=None, range=Optional[str])

slots.count_fish_collected = Slot(uri=SAVED.count_fish_collected, name="count_fish_collected", curie=SAVED.curie('count_fish_collected'),
                   model_uri=SAVED.count_fish_collected, domain=None, range=Optional[float])

slots.fish_length = Slot(uri=SAVED.fish_length, name="fish_length", curie=SAVED.curie('fish_length'),
                   model_uri=SAVED.fish_length, domain=None, range=Optional[float])

slots.fish_mass = Slot(uri=SAVED.fish_mass, name="fish_mass", curie=SAVED.curie('fish_mass'),
                   model_uri=SAVED.fish_mass, domain=None, range=Optional[float])

slots.fish_species_common = Slot(uri=SAVED.fish_species_common, name="fish_species_common", curie=SAVED.curie('fish_species_common'),
                   model_uri=SAVED.fish_species_common, domain=None, range=Optional[str])

slots.fish_species_scientific = Slot(uri=SAVED.fish_species_scientific, name="fish_species_scientific", curie=SAVED.curie('fish_species_scientific'),
                   model_uri=SAVED.fish_species_scientific, domain=None, range=Optional[str])

slots.lice_af_average = Slot(uri=SAVED.lice_af_average, name="lice_af_average", curie=SAVED.curie('lice_af_average'),
                   model_uri=SAVED.lice_af_average, domain=None, range=Optional[float])

slots.lice_af_total = Slot(uri=SAVED.lice_af_total, name="lice_af_total", curie=SAVED.curie('lice_af_total'),
                   model_uri=SAVED.lice_af_total, domain=None, range=Optional[int])

slots.lice_density_collected = Slot(uri=SAVED.lice_density_collected, name="lice_density_collected", curie=SAVED.curie('lice_density_collected'),
                   model_uri=SAVED.lice_density_collected, domain=None, range=Optional[float])

slots.northing = Slot(uri=SAVED.northing, name="northing", curie=SAVED.curie('northing'),
                   model_uri=SAVED.northing, domain=None, range=Optional[int])

slots.easting = Slot(uri=SAVED.easting, name="easting", curie=SAVED.curie('easting'),
                   model_uri=SAVED.easting, domain=None, range=Optional[int])

slots.national_grid_reference = Slot(uri=SAVED.national_grid_reference, name="national_grid_reference", curie=SAVED.curie('national_grid_reference'),
                   model_uri=SAVED.national_grid_reference, domain=None, range=Optional[str])

slots.latitude = Slot(uri=SAVED.latitude, name="latitude", curie=SAVED.curie('latitude'),
                   model_uri=SAVED.latitude, domain=None, range=Optional[str])

slots.longitude = Slot(uri=SAVED.longitude, name="longitude", curie=SAVED.curie('longitude'),
                   model_uri=SAVED.longitude, domain=None, range=Optional[str])

slots.global_coordinate_system = Slot(uri=SAVED.global_coordinate_system, name="global_coordinate_system", curie=SAVED.curie('global_coordinate_system'),
                   model_uri=SAVED.global_coordinate_system, domain=None, range=Optional[str])

slots.depth = Slot(uri=SAVED.depth, name="depth", curie=SAVED.curie('depth'),
                   model_uri=SAVED.depth, domain=None, range=Optional[float])

slots.series = Slot(uri=SAVED.series, name="series", curie=SAVED.curie('series'),
                   model_uri=SAVED.series, domain=None, range=Optional[str])

slots.notes = Slot(uri=SAVED.notes, name="notes", curie=SAVED.curie('notes'),
                   model_uri=SAVED.notes, domain=None, range=Optional[str])

slots.site_name = Slot(uri=SAVED.site_name, name="site_name", curie=SAVED.curie('site_name'),
                   model_uri=SAVED.site_name, domain=None, range=Optional[str])

slots.site_id = Slot(uri=SAVED.site_id, name="site_id", curie=SAVED.curie('site_id'),
                   model_uri=SAVED.site_id, domain=None, range=Optional[str])

slots.cage_id = Slot(uri=SAVED.cage_id, name="cage_id", curie=SAVED.curie('cage_id'),
                   model_uri=SAVED.cage_id, domain=None, range=Optional[str])

slots.atomic_name = Slot(uri=SAVED.atomic_name, name="atomic_name", curie=SAVED.curie('atomic_name'),
                   model_uri=SAVED.atomic_name, domain=None, range=URIRef,
                   pattern=re.compile(r'^:?[a-z]+[[a-z]|_|]*$'))

slots.hash = Slot(uri=SAVED.hash, name="hash", curie=SAVED.curie('hash'),
                   model_uri=SAVED.hash, domain=None, range=Optional[str])

slots.resource_hash = Slot(uri=SAVED.resource_hash, name="resource_hash", curie=SAVED.curie('resource_hash'),
                   model_uri=SAVED.resource_hash, domain=None, range=str)

slots.path = Slot(uri=SAVED.path, name="path", curie=SAVED.curie('path'),
                   model_uri=SAVED.path, domain=None, range=Optional[Union[str, URI]])

slots.resource_path = Slot(uri=SAVED.resource_path, name="resource_path", curie=SAVED.curie('resource_path'),
                   model_uri=SAVED.resource_path, domain=None, range=Union[str, URI])

slots.schema_path_yaml = Slot(uri=SAVED.schema_path_yaml, name="schema_path_yaml", curie=SAVED.curie('schema_path_yaml'),
                   model_uri=SAVED.schema_path_yaml, domain=None, range=Union[str, URI])

slots.schema_path_ttl = Slot(uri=SAVED.schema_path_ttl, name="schema_path_ttl", curie=SAVED.curie('schema_path_ttl'),
                   model_uri=SAVED.schema_path_ttl, domain=None, range=Optional[Union[str, URI]])

slots.tables = Slot(uri=SAVED.tables, name="tables", curie=SAVED.curie('tables'),
                   model_uri=SAVED.tables, domain=None, range=Union[Dict[Union[str, TableDescAtomicName], Union[dict, TableDesc]], List[Union[dict, TableDesc]]])

slots.column = Slot(uri=SAVED.column, name="column", curie=SAVED.curie('column'),
                   model_uri=SAVED.column, domain=None, range=str)

slots.variable = Slot(uri=SAVED.variable, name="variable", curie=SAVED.curie('variable'),
                   model_uri=SAVED.variable, domain=None, range=Union[str, ExtColumnDescAtomicName])

slots.table = Slot(uri=SAVED.table, name="table", curie=SAVED.curie('table'),
                   model_uri=SAVED.table, domain=None, range=Union[str, TableDescAtomicName])

slots.job_scope = Slot(uri=SAVED.job_scope, name="job_scope", curie=SAVED.curie('job_scope'),
                   model_uri=SAVED.job_scope, domain=None, range=Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]])

slots.job_scope_descriptive = Slot(uri=SAVED.job_scope_descriptive, name="job_scope_descriptive", curie=SAVED.curie('job_scope_descriptive'),
                   model_uri=SAVED.job_scope_descriptive, domain=None, range=Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]])

slots.job_scope_collected = Slot(uri=SAVED.job_scope_collected, name="job_scope_collected", curie=SAVED.curie('job_scope_collected'),
                   model_uri=SAVED.job_scope_collected, domain=None, range=Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]])

slots.job_scope_modelled = Slot(uri=SAVED.job_scope_modelled, name="job_scope_modelled", curie=SAVED.curie('job_scope_modelled'),
                   model_uri=SAVED.job_scope_modelled, domain=None, range=Optional[Union[Union[dict, ScopeDesc], List[Union[dict, ScopeDesc]]]])

slots.job_type = Slot(uri=SAVED.job_type, name="job_type", curie=SAVED.curie('job_type'),
                   model_uri=SAVED.job_type, domain=None, range=Union[str, "JobType"])

slots.jobs = Slot(uri=SAVED.jobs, name="jobs", curie=SAVED.curie('jobs'),
                   model_uri=SAVED.jobs, domain=None, range=Union[Dict[Union[str, JobDescAtomicName], Union[dict, JobDesc]], List[Union[dict, JobDesc]]])

slots.gcp_source = Slot(uri=SAVED.gcp_source, name="gcp_source", curie=SAVED.curie('gcp_source'),
                   model_uri=SAVED.gcp_source, domain=None, range=Optional[str])

slots.local_version = Slot(uri=SAVED.local_version, name="local_version", curie=SAVED.curie('local_version'),
                   model_uri=SAVED.local_version, domain=None, range=Optional[str])

slots.count_fish_interpolated = Slot(uri=SAVED.count_fish_interpolated, name="count_fish_interpolated", curie=SAVED.curie('count_fish_interpolated'),
                   model_uri=SAVED.count_fish_interpolated, domain=None, range=Optional[float])

slots.lice_density_modelled = Slot(uri=SAVED.lice_density_modelled, name="lice_density_modelled", curie=SAVED.curie('lice_density_modelled'),
                   model_uri=SAVED.lice_density_modelled, domain=None, range=Optional[float])
