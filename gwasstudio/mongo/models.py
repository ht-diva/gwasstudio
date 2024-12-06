import datetime
from enum import Enum

from mongoengine import (
    DateTimeField,
    Document,
    EnumField,
    IntField,
    ListField,
    ReferenceField,
    StringField,
)

from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.connection_manager import get_mec
from gwasstudio.mongo.mixin import MongoMixin

# TODO: This configuration manager doesn't use a custom configuration file passed by cli
cm = ConfigurationManager()
DataCategory = Enum(
    "DataCategory",
    {item.replace(" ", "_").upper(): item for item in cm.get_data_category_list},
)
Ancestry = Enum("Ancestry", {item.replace(" ", "_").upper(): item for item in cm.get_ancestry_list})
Build = Enum("Build", {item.replace(" ", "_").upper(): item for item in cm.get_build_list})


class Metadata(Document):
    creation_date = DateTimeField(default=datetime.datetime.now())
    modification_date = DateTimeField()
    category = EnumField(DataCategory)
    tags = ListField(StringField(max_length=50))

    meta = {"allow_inheritance": True}


class Publication(Metadata):
    title = StringField(max_length=200, sparse=True, required=True, unique=True)
    ext_uid = StringField(max_length=200, sparse=True, unique=True)  # doi:..., pmid:...


class DataProfile(Metadata):
    """
    uniqueness of the trait is ensured by project+data_id
    """

    project = StringField(max_length=500, required=True)
    data_id = StringField(max_length=200, unique_with="project", required=True)
    trait_desc = StringField(max_length=500)
    total_samples = IntField()
    total_cases = IntField()
    population = EnumField(Ancestry)
    references = ListField(ReferenceField(Publication))
    build = EnumField(Build)


class EnhancedDataProfile(MongoMixin):
    def __init__(self, **kwargs):
        self._klass = kwargs.get("klass", DataProfile)
        self._mec = kwargs.get("mec", get_mec())
        self._obj = self._klass(
            project=kwargs.get("project"),
            data_id=kwargs.get("data_id"),
            trait_desc=kwargs.get("trait_desc", None),
            category=kwargs.get("category"),
            tags=kwargs.get("tags", []),
            total_samples=kwargs.get("total_samples"),
            total_cases=kwargs.get("total_cases"),
            population=kwargs.get("population", "NR"),
            references=kwargs.get("references", []),
            build=kwargs.get("build", None),
        )

    # required attributes
    @property
    def mec(self):
        return self._mec

    @property
    def mdb_obj(self):
        return self._obj

    @property
    def klass(self):
        return self._klass

    @property
    def pk(self):
        """
        the primary key of the object known by MongoDB (a.k.a _id)
        """
        return self._obj.pk

    @property
    def unique_key(self):
        return f"{self.mdb_obj.project}:{self.mdb_obj.data_id}"

    @unique_key.setter
    def unique_key(self, uk):
        separator = ":"
        self._obj.project = uk.split(separator)[0]
        self._obj.data_id = uk.split(separator)[1]

    # end of required
