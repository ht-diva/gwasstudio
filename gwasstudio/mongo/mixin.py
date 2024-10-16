import datetime

from gwasstudio import logger


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
            logger.warning("Document {} does not exist on remote, " "skipping {} operation".format(self.uk, op))
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
        with self.mec:
            if hasattr(self.mdb_obj, "modification_date"):
                self.mdb_obj.modification_date = datetime.datetime.now()
            self.mdb_obj.save(**kwargs)
        logger.info("{} saved".format(self.unique_key))

    def view(self):
        """
        Return object's detail in JSON format
        """
        detail = {}
        if self.ensure_is_mapped("view"):
            with self.mec:
                detail = self.klass.objects(unique_key=self.uk).as_pymongo()[0]
                logger.debug(detail)
        return detail

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
                    logger.info("{} modified".format(self.uk))
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
                logger.info("{} deleted".format(self.uk))
            self.mdb_obj.id = None
