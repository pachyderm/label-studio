"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import json
import logging
import os
from pathlib import Path
from subprocess import run, Popen
from time import sleep

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from rest_framework.exceptions import ValidationError

from io_storages.base_models import (
      ExportStorage,
      ExportStorageLink,
      ImportStorage,
      ImportStorageLink,
)
from tasks.models import Annotation

PFS_DIR = Path("/pfs")
logger = logging.getLogger(__name__)


class PachydermMixin(models.Model):
    repository = models.TextField(
        _('repository'), null=True, blank=True,
        help_text='Local path')

    def validate_connection(self):
        if not PFS_DIR.is_dir():
            raise ValidationError(f"Mount directory {PFS_DIR} does not exist.")
        if run(["pachctl", "list", "branch", self.repository], check=True).returncode:
            raise ValidationError(f"Pachyderm repo not found: {self.repository}")


class PachydermImportStorage(PachydermMixin, ImportStorage):
    url_scheme = 'https'

    def can_resolve_url(self, url):
        return False

    def iterkeys(self):
        path = PFS_DIR / str(self.repository) / str(self.repository)
        for file in path.rglob('*'):
            if file.is_file():
                yield str(file)

    def get_data(self, key):
        relative_path = str(Path(key).relative_to(PFS_DIR))
        return {settings.DATA_UNDEFINED_NAME: f'{settings.HOSTNAME}/data/pfs/?d={relative_path}'}

    def scan_and_create_links(self):
        return self._scan_and_create_links(PachydermImportStorageLink)

    def sync(self):
        mount_point = PFS_DIR / str(self.repository)
        mount_point.mkdir(exist_ok=True)
        local_path = mount_point / str(self.repository)
        if not local_path.exists():
            self.process = Popen(["pachctl", "mount", "-r", self.repository, str(mount_point)])
            for _ in range(30):
                if local_path.exists():
                    break
                sleep(1)
        self.scan_and_create_links()


class PachydermExportStorage(ExportStorage, PachydermMixin):

    def save_annotation(self, annotation):
        logger.debug(f'Creating new object on {self.__class__.__name__} Storage {self} for annotation {annotation}')
        ser_annotation = self._get_serialized_data(annotation)

        # get key that identifies this object in storage
        key = PachydermExportStorageLink.get_key(annotation)
        key = os.path.join(PFS_DIR, str(self.repository), str(self.repository), f"{key}.json")

        # put object into storage
        with open(key, mode='w') as f:
            json.dump(ser_annotation, f, indent=2)

        # Create export storage link
        PachydermExportStorageLink.create(annotation, self)

    def sync(self):
        self.save_all_annotations()
        mount_point = PFS_DIR / str(self.repository)
        mount_point.mkdir(exist_ok=True)
        local_path = mount_point / str(self.repository)
        if local_path.exists():
            run(["pachctl", "unmount", str(mount_point)])
        self.process = Popen(["pachctl", "mount", "-r", f"{self.repository}+w", str(mount_point)])
        for _ in range(30):
            if local_path.exists():
                break
            sleep(1)


class PachydermImportStorageLink(ImportStorageLink):
    storage = models.ForeignKey(PachydermImportStorage, on_delete=models.CASCADE, related_name='links')


class PachydermExportStorageLink(ExportStorageLink):
    storage = models.ForeignKey(PachydermExportStorage, on_delete=models.CASCADE, related_name='links')


@receiver(post_save, sender=Annotation)
def export_annotation_to_local_files(sender, instance, **kwargs):
    project = instance.task.project
    if hasattr(project, 'io_storages_pachydermexportstorages'):
        for storage in project.io_storages_pachydermexportstorages.all():
            logger.debug(f'Export {instance} to Local Storage {storage}')
            storage.save_annotation(instance)
