import datetime
import json

from mongoengine.errors import NotUniqueError
from mongoengine.queryset.visitor import Q

from gwasstudio import logger
from gwasstudio.utils import find_item


class MongoMixin:
    def __init_subclass__(cls, **kwargs):
        attr_to_check = ["mdb_obj", "klass", "mec", "pk", "unique_key"]
        for a in attr_to_check:
            if not hasattr(cls, a):
                print(vars(cls))
                raise Exception(f"class {cls.__name__} is missing a {a} attribute.")
        return super().__init_subclass__(**kwargs)

    @property
    def is_connected(self):
        return False if self.mec is None else True

    @property
    def is_mapped(self):
        with self.mec:
            obj = self.klass.objects(project=self.mdb_obj.project, data_id=self.mdb_obj.data_id).first()
            return bool(obj)
            # return False

    def map(self):
        with self.mec:
            objs = self.klass.objects(project=self.mdb_obj.project, data_id=self.mdb_obj.data_id)
            if (objs.count()) == 1:
                msg = "mapping, {} DataIdentifier found".format(len(objs))
                logger.debug(msg)
                self.mdb_obj.id = objs[0].id
                return True
            return False

    def ensure_is_mapped(self, op=None):
        if not self.map():
            logger.warning("Document {} does not exist on remote, skipping {} operation".format(self.unique_key, op))
            return False
        return True

    def save(self, **kwargs):
        """
        Save the Document to the database. If the document already exists,
        it will be updated, otherwise it will be created.
        Returns the saved object instance.
        :param kwargs:
        :return: DataObject
        """
        if not self.is_mapped:
            self.map()

        if hasattr(self.mdb_obj, "modification_date"):
            self.mdb_obj.modification_date = datetime.datetime.now()
        try:
            with self.mec:
                self.mdb_obj.save(**kwargs)
            logger.info("{} saved".format(self.unique_key))
        except NotUniqueError:
            self.ensure_is_mapped("save")
            with self.mec:
                self.mdb_obj.save(**kwargs)
            logger.warning("{} updated, as it was not a unique ID".format(self.unique_key))

    def view(self):
        """
        Return object's detail in JSON format
        """
        detail = {}
        if self.ensure_is_mapped("view"):
            with self.mec:
                detail = self.klass.objects(id=self.pk).as_pymongo()[0]
                logger.debug(detail)
        return detail

    def query(self, case_sensitive=False, exact_match=False, **kwargs):
        """
        Queries the database based on the provided keyword arguments.

        Args:
            case_sensitive (bool, optional): Whether the query should be case-sensitive. Defaults to False.
            exact_match (bool, optional): Whether the query should force exact matches for all fields (no substring matches). Defaults to False.
            **kwargs: Additional keyword arguments to filter the query results.

        Returns:
            list: A list of query results.
        """
        operators = {"case_insensitive": ["iexact", "icontains"], "case_sensitive": ["exact", "contains"]}
        exact_op, contains_op = operators["case_sensitive" if case_sensitive else "case_insensitive"]

        docs = []
        if not kwargs:
            return []

        jds = {field: kwargs.pop(field, {}) for field in self.klass.json_dict_fields() if field in kwargs}

        # Compose queries from data_ids list, if any.
        queries = [Q(**{f"data_id__{exact_op}": value}) for value in kwargs.pop("data_ids", [])]

        # Compose queries from regular key value pairs in the yaml file.
        query_fields_exact = {f"{key}__{exact_op}": value for key, value in kwargs.items()}
        if query_fields_exact:
            queries.append(Q(**query_fields_exact))
        logger.debug(queries)

        with self.mec:
            if len(jds.keys()) > 0:  # there are JSON fields to query
                for jdk, jdv in jds.items():
                    query_fields_contains = {f"{jdk}__{contains_op}.{key}": value for key, value in jdv.items()}

                    for key, value in query_fields_contains.items():
                        query_field_contains = key.split(".")[0]
                        queries.append(Q(**{query_field_contains: value}))

                    # Use & operator to combine all the queries with AND logic
                    query_args = Q()
                    for q in queries:
                        query_args = query_args & q
                    logger.debug(query_args)
                    all_docs = list(self.klass.objects(query_args).as_pymongo())
                    if exact_match:
                        # exact_math: exact match for all JSON fields' key-values
                        for qr in all_docs:
                            data = json.loads(qr.get(jdk, "{}"))
                            if all(str(find_item(data, k)).strip() == str(v).strip() for k, v in jdv.items()):
                                docs.append(qr)
                    else:
                        # not exact_match: substring match for JSON fields
                        docs.extend(
                            qr
                            for qr in all_docs
                            if any(
                                value.casefold() in find_item(json.loads(qr[jdk]), key.split(".").pop()).casefold()
                                for key, value in jdv.items()
                                if isinstance(value, str)
                            )
                        )
            else:
                for query_arg in queries:
                    logger.debug(query_arg)
                    docs.extend(qr for qr in self.klass.objects(query_arg).as_pymongo())
            logger.debug(f"found {len(docs)} documents")

        return docs

    def modify(self, **kwargs):
        """
        Perform an atomic update of the document in the database and
        reload the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn’t match the query.
        """
        result = False
        if self.ensure_is_mapped("modify"):
            if len(kwargs) > 0:
                with self.mec:
                    self.mdb_obj.modification_date = datetime.datetime.now()
                    result = self.mdb_obj.modify(**kwargs)
                    logger.info("{} modified".format(self.unique_key))
            else:
                logger.warning("No attributes to update, skipping the operation")
        return result

    def delete(self, **kwargs):
        """
        Delete the Document from the database and unmap the local object.
        This will only take effect if the document has been previously saved.
        """
        if self.ensure_is_mapped("delete"):
            with self.mec:
                self.mdb_obj.delete(**kwargs)
                logger.info("{} deleted".format(self.unique_key))
            self.mdb_obj.id = None
