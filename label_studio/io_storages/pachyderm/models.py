"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import json
import logging
import signal
import os
from pathlib import Path
from subprocess import run, Popen
from time import sleep
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from pachyderm_sdk import Client
from pachyderm_sdk.api import pfs
from rest_framework.exceptions import ValidationError

from io_storages.base_models import (
      ExportStorage,
      ExportStorageLink,
      ImportStorage,
      ImportStorageLink,
      ProjectStorageMixin,
)
from tasks.models import Annotation

PFS_DIR = Path("/pfs")
logger = logging.getLogger(__name__)

clients_cache = {}


class PachydermMixin(models.Model):
    pach_project = models.TextField(_('project'), blank=True, help_text="Project")
    pach_repo = models.TextField(_('repository'), blank=True, help_text='Repository')
    pach_branch = models.TextField(_('branch'), blank=True, help_text='Branch')
    pach_commit = models.TextField(_('commit'), blank=True, help_text='Commit')
    pachd_address = models.TextField(_('pachyderm_address'), blank=True, help_text='Pachyderm Address')
    use_blob_urls = models.BooleanField(
        _('use_blob_urls'), default=False,
        help_text='Interpret objects as BLOBs and generate URLs'
    )

    def get_client(self):
        if self.pachd_address in clients_cache:
            return clients_cache[self.pachd_address]

        client = Client.from_pachd_address(pachd_address=str(self.pachd_address))
        clients_cache[self.pachd_address] = client
        return client

    @property
    def branch(self) -> pfs.Commit:
        return pfs.Branch.from_uri(
            f"{self.pach_project}/{self.pach_repo}@{self.pach_branch}"
        )

    @property
    def commit(self) -> pfs.Commit:
        return pfs.Commit.from_uri(
            f"{self.pach_project}/{self.pach_repo}@{self.pach_commit}"
        )

    def clean(self):
        """
        Hook for doing any extra model-wide validation after clean() has been
        called on every field by self.clean_fields. Any ValidationError raised
        by this method will not be associated with a particular field; it will
        have a special-case association with the field defined by NON_FIELD_ERRORS.
        """
        if not self.pach_project:
            self.pach_project = "default"
        if not self.pach_branch:
            self.pach_branch = "master"
        if not self.pach_commit:
            client = self.get_client()
            branch = pfs.Branch.from_uri(f"{self.pach_repo}@{self.pach_branch}")
            branch_info = client.pfs.inspect_branch(branch=branch)
            self.pach_commit = branch_info.head.id
        super().clean()

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        self.clean()
        super().save(force_insert, force_update, using, update_fields)

    def validate_connection(self, client = None):
        logger.debug('validate_connection')
        self.clean()
        if client is None:
            client = self.get_client()
        if not client.pfs.commit_exists(self.commit):
            raise ValidationError(f"Commit {self.commit} does not exist.")


class PachydermImportStorageBase(PachydermMixin, ImportStorage):
    url_scheme = 'http'

    def iterkeys(self):
        client = self.get_client()
        base = pfs.File(commit=self.commit, path="/")
        for file_info in client.pfs.list_file(file=base):
            yield file_info.file.path

    def get_data(self, key):
        client = self.get_client()
        file = pfs.File(commit=self.commit, path=key)

        if self.use_blob_urls:
            result = run(['pachctl', 'misc', 'generate-download-url', file.as_uri()], capture_output=True)
            data_key = settings.DATA_UNDEFINED_NAME
            redirect = f"{self.url_scheme}://{self.pachd_address}/archive/{result.stdout.decode().strip()}.zip"
            archive_path = file.as_uri().replace("@", "/").replace(":", "")
            return {data_key: f'{settings.HOSTNAME}/data/pfs/?redirect={redirect}&d={archive_path}'}

        with client.pfs.pfs_file(file) as obj:
            value = json.loads(obj)
        if not isinstance(value, dict):
            raise ValueError(f"Error on key {key}: For {self.__class__.__name__} your JSON file must be a dictionary with one task.")  # noqa
        return value

    def scan_and_create_links(self):
        return self._scan_and_create_links(PachydermImportStorageLink)

    class Meta:
        abstract = True


class PachydermImportStorage(ProjectStorageMixin, PachydermImportStorageBase):

    class Meta:
        abstract = False


class PachydermExportStorage(PachydermMixin, ExportStorage):

    def save_annotation(self, annotation):
        client = self.get_client()
        logger.debug(f'Creating new object on {self.__class__.__name__} Storage {self} for annotation {annotation}')
        ser_annotation = self._get_serialized_data(annotation)

        # get key that identifies this object in storage
        key = PachydermExportStorageLink.get_key(annotation)

        # put object into storage
        with client.pfs.commit(branch=self.branch) as commit:
            commit.put_file_from_bytes(path=key, data=json.dumps(ser_annotation, indent=2))

        # Create export storage link
        PachydermExportStorageLink.create(annotation, self)


class PachydermImportStorageLink(ImportStorageLink):
    storage = models.ForeignKey(PachydermImportStorage, on_delete=models.CASCADE, related_name='links')


class PachydermExportStorageLink(ExportStorageLink):
    storage = models.ForeignKey(PachydermExportStorage, on_delete=models.CASCADE, related_name='links')


@receiver(post_save, sender=Annotation)
def export_annotation_to_pfs(sender, instance, **kwargs):
    project = instance.task.project
    if hasattr(project, 'io_storages_pachydermexportstorages'):
        for storage in project.io_storages_pachydermexportstorages.all():
            logger.debug(f'Export {instance} to Local Storage {storage}')
            storage.save_annotation(instance)
