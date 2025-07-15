import datetime
import json
from enum import Enum

from mongoengine import DateTimeField, Document, EnumField, ListField, ReferenceField, StringField, ValidationError

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


class JSONField(StringField):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_mongo(self, value):
        if value is not None:
            try:
                return json.dumps(value)
            except TypeError as e:
                raise ValidationError("Invalid JSON value: {}".format(e))
        return value

    def to_python(self, value):
        if value is not None and isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                raise ValidationError("Invalid JSON string: {}".format(e))
        return value

    def validate(self, value):
        if value is not None and not isinstance(value, (str, dict, list)):
            raise ValidationError("Invalid JSON value")


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
    uniqueness of the record is ensured by project+study+data_id
    """

    project = StringField(max_length=250, required=True)
    study = StringField(max_length=250, required=True)
    data_id = StringField(max_length=250, unique_with=["project", "study"], required=True)
    trait = JSONField()
    total = JSONField()
    population = ListField(StringField(max_length=250))
    references = ListField(ReferenceField(Publication))
    build = EnumField(Build)
    notes = JSONField()

    @staticmethod
    def json_dict_fields() -> tuple:
        """
        Returns a tuple of field names in this metadata class that store JSON-formatted data.

        These fields are stored as strings in MongoDB, where each string contains a valid JSON object.
        The purpose of this method is to provide a convenient way to access these JSON-formatted fields.

        :return: A tuple of field names (str) that store JSON-formatted data
        """
        return tuple(field.name for field in DataProfile._fields.values() if isinstance(field, JSONField))

    @staticmethod
    def listfield_names() -> tuple:
        """
        Returns a tuple of field names in this metadata class that store ListField objects.
        """
        return tuple(field.name for field in DataProfile._fields.values() if isinstance(field, ListField))


class EnhancedDataProfile(MongoMixin):
    def __init__(self, **kwargs):
        self._klass = kwargs.get("klass", DataProfile)
        uri = kwargs.get("uri", None)
        self._mec = get_mec(uri=uri) if uri else kwargs.get("mec", get_mec())

        self._obj = self._klass(
            project=kwargs.get("project"),
            study=kwargs.get("study"),
            data_id=kwargs.get("data_id"),
            trait=kwargs.get("trait", None),
            category=kwargs.get("category"),
            tags=kwargs.get("tags", []),
            total=kwargs.get("total", None),
            population=kwargs.get("population", []),
            references=kwargs.get("references", []),
            build=kwargs.get("build", None),
            notes=kwargs.get("notes", None),
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
        return f"{self.mdb_obj.project}:{self.mdb_obj.study}:{self.mdb_obj.data_id}"

    @unique_key.setter
    def unique_key(self, uk):
        separator = ":"
        project, study, data_id = uk.split(separator)
        self._obj.project = project
        self._obj.study = study
        self._obj.data_id = data_id

    # end of required
