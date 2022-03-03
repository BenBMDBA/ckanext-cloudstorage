#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
import os.path
#import urlparse
from ast import literal_eval
from datetime import datetime, timedelta
import logging
import werkzeug

#from pylons import config
#from ckan.common import config
from ckantoolkit import config
from ckan import model
from ckan.lib import munge
import ckan.plugins as p

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver


class CloudStorage(object):
    def __init__(self):
        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    def path_from_filename(self, rid, filename):
        raise NotImplemented

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return literal_eval(config['ckanext.cloudstorage.driver_options'])

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config['ckanext.cloudstorage.driver']

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config['ckanext.cloudstorage.container_name']

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.use_secure_urls', False)
        )

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.leave_files', False)
        )

    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if True: #self.driver_name == 'AZURE_BLOBS': #forced on by ben 2/3/22
            logger = logging.getLogger(__name__)
            logger.debug('about to check for azure storage')
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage
                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                logger = logging.getLogger(__name__)
                logger.debug('import error for azure storage')
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if 'S3' in self.driver_name:
            try:
                # Yes? Is the boto package available?
                import boto
                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get('ckanext.cloudstorage.guess_mimetype', False)
        )


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop('upload', None)
        self._clear = resource.pop('clear_upload', None)
        multipart_name = resource.pop('multipart_name', None)
        logger = logging.getLogger(__name__)
        
        logger.debug('upload_field_storage = %s',upload_field_storage)
        logger.debug('upload_field_storage type = %s',type(upload_field_storage))
        logger.debug('upload_field_storage dir = %s',dir(upload_field_storage))
        logger.debug('ckanext-cloudstorage about to check is instance')
        # Check to see if a file has been provided
        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = upload_field_storage.file
            logger = logging.getLogger(__name__)
            logger.debug('ckanext-cloudstorage is instance triggered')
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
            resource['last_modified'] = datetime.utcnow()
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource['url'] = munge.munge_filename(multipart_name)
            resource['url_type'] = 'upload'
        elif self._clear and resource.get('id'):
            logger = logging.getLogger(__name__)
            logger.debug('ckanext-cloudstorage created-but-not-commited resource')
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(
                model.Resource
            ).get(
                resource['id']
            )

            self.old_filename = old_resource.url
            resource['url_type'] = ''
        elif isinstance(upload_field_storage, werkzeug.datastructures.FileStorage):
            logger = logging.getLogger(__name__)
            logger.debug('ckanext-cloudstorage werkzeug is instance triggered')
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = upload_field_storage.stream
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
            resource['last_modified'] = datetime.utcnow()


    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join(
            'resources',
            rid,
            munge.munge_filename(filename)
        )

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        logger = logging.getLogger(__name__)
        logger.debug('upload in ckanext-cloudstorage triggered ')
        logger.debug('%s is our file', self.filename)
        if self.filename:
            logger.debug('has filename')
            if self.can_use_advanced_azure:
                from azure.storage import blob as azure_blob
                from azure.storage.blob.models import ContentSettings
                logger.debug('using advanced azure')
                
                from azure.storage.blob import BlobServiceClient
                connectionstring= "DefaultEndpointsProtocol=https;AccountName=" + self.driver_options['key'] + ";AccountKey=" + self.driver_options['secret']
                logger.debug('%s is our connection string', connectionstring)
                blob_service_client = BlobServiceClient.from_connection_string(connectionstring)
                container_client = blob_service_client.get_container_client(self.container_name)
            
                #blob_client = container_client.upload_blob(name =path, data = data)

#                blob_service = azure_blob.BlockBlobService(
#                    self.driver_options['key'],
#                    self.driver_options['secret']
#                )
                content_settings = None
                if self.guess_mimetype:
                    content_type, _ = mimetypes.guess_type(self.filename)
                    if content_type:
                        content_settings = ContentSettings(
                            content_type=content_type
                        )
                return container_client.upload_blob(
                    name =self.path_from_filename(
                         id,
                        self.filename
                    ),
                    data = self.file_upload)
                #return blob_service.create_blob_from_stream(
                #    container_name=self.container_name,
                #    blob_name=self.path_from_filename(
                #        id,
                #        self.filename
                #    ),
                #   stream=self.file_upload,
                #    content_settings=content_settings
                #)
            else:
                self.container.upload_object_via_stream(
                    self.file_upload,
                    object_name=self.path_from_filename(
                        id,
                        self.filename
                    )
                )

        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            try:
                self.container.delete_object(
                    self.container.get_object(
                        self.path_from_filename(
                            id,
                            self.old_filename
                        )
                    )
                )
            except ObjectDoesNotExistError:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def get_url_from_filename(self, rid, filename):
        """
        Retrieve a publically accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.

        :returns: Externally accessible URL or None.
        """
        # Find the key the file *should* be stored at.
        path = self.path_from_filename(rid, filename)

        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
            from azure.storage import blob as azure_blob
            
            blob_service = azure_blob.BlockBlobService(
                self.driver_options['key'],
                self.driver_options['secret']
            )

            return blob_service.make_blob_url(
                container_name=self.container_name,
                blob_name=path,
                sas_token=blob_service.generate_blob_shared_access_signature(
                    container_name=self.container_name,
                    blob_name=path,
                    expiry=datetime.utcnow() + timedelta(hours=1),
                    permission=azure_blob.BlobPermissions.READ
                )
            )
        elif self.can_use_advanced_aws and self.use_secure_urls:
            from boto.s3.connection import S3Connection
            s3_connection = S3Connection(
                self.driver_options['key'],
                self.driver_options['secret']
            )
            return s3_connection.generate_url(
                expires_in=60 * 60,
                method='GET',
                bucket=self.container_name,
                query_auth=True,
                key=path
            )

        # Find the object for the given key.
        obj = self.container.get_object(path)
        if obj is None:
            return

        # Not supported by all providers!
        try:
            return self.driver.get_object_cdn_url(obj)
        except NotImplementedError:
            if 'S3' in self.driver_name:
                return urlparse.urljoin(
                    'https://' + self.driver.connection.host,
                    '{container}/{path}'.format(
                        container=self.container_name,
                        path=path
                    )
                )
            # This extra 'url' property isn't documented anywhere, sadly.
            # See azure_blobs.py:_xml_to_object for more.
            elif 'url' in obj.extra:
                return obj.extra['url']
            raise

    @property
    def package(self):
        return model.Package.get(self.resource['package_id'])
