"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import json
import logging
import os
from pathlib import Path
from time import sleep
from typing import Dict, Tuple

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
from io_storages.pachyderm.mount_server import (
    get_repos, mount_repo, unmount_repo, safe_start_mount_server
)
from tasks.models import Annotation

PFS_DIR = Path("/pfs")
logger = logging.getLogger(__name__)

_mounts: Dict[int, str] = dict()


class PachydermMixin(models.Model):
    repository = models.TextField(_('repository'), blank=True, help_text='Local path')
    use_blob_urls = models.BooleanField(
        _('use_blob_urls'), default=False,
        help_text='Interpret objects as BLOBs and generate URLs')

    @property
    def is_mounted(self) -> bool:
        return os.path.exists(str(self.mount_point))

    @property
    def mount_point(self) -> Path:
        return PFS_DIR / str(self.repository_with_branch)

    @property
    def repository_with_branch(self) -> str:
        repo_name, branch = split_branch(str(self.repository))
        branch = branch or "master"
        return f"{repo_name}@{branch}"

    def mount(self, wait: int = 30, *, writable: bool = False) -> None:
        """Mount the pachyderm repository and update state."""
        safe_start_mount_server()
        repository_with_branch = str(self.repository_with_branch)
        repo_name, branch = split_branch(repository_with_branch)

        # If this entry is being edited, unmount the previous repo if it exists.
        mounted_repo = _mounts.get(self.pk, None)
        if mounted_repo is not None and mounted_repo != repository_with_branch:
            mounted_repo_name, mounted_branch = split_branch(mounted_repo)
            unmount_repo(mounted_repo_name, mounted_branch, name=repository_with_branch)

        logger.debug(f"Mounting repository: {repository_with_branch}")
        if not self.is_mounted:
            mode = "rw" if writable else "r"
            _mounts[self.pk] = mount_repo(repo_name, branch, mode, name=repository_with_branch)
            sleep(1)
            for _ in range(wait):
                if self.is_mounted:
                    return
                sleep(1)
            raise TimeoutError(f"Could not mount repository: {repository_with_branch}")

    def unmount(self) -> None:
        """Unmount the pachyderm repository and update state."""
        safe_start_mount_server()
        repository_with_branch = str(self.repository_with_branch)
        repo_name, branch = split_branch(repository_with_branch)

        logger.debug(f"Unmounting repository: {repository_with_branch}")
        unmount_repo(repo_name, branch, name=repository_with_branch)
        del _mounts[self.pk]

    def delete(self, *args, **kwargs):
        """Deletes the database entry for this storage device and unmounts the repo."""
        if self.is_mounted:
            self.unmount()
        super().delete(*args, **kwargs)

    def validate_connection(self):
        """Validates the pachyderm repository and mount-server."""
        safe_start_mount_server()
        if not PFS_DIR.is_dir():
            raise ValidationError(f"Mount directory {PFS_DIR} does not exist.")

        repo_name, branch = split_branch(str(self.repository_with_branch))
        repositories = get_repos()
        repository = repositories.get(repo_name, None)
        if not repository:
            raise ValidationError(f"Pachyderm repo not found: {repo_name}")

        if branch not in repository.branches:
            raise ValidationError(
                f"branch/commit {branch} not found for Pachyderm repo {repo_name}"
            )


class PachydermImportStorage(PachydermMixin, ImportStorage):
    url_scheme = 'https'

    def can_resolve_url(self, url):
        return False

    def iterkeys(self):
        """Iterate through all files in the mount."""
        for file in self.mount_point.rglob('*'):
            if file.is_file():
                yield str(file)

    def get_data(self, key):
        """This method returns a url that points to specified pachyderm datum."""
        if self.use_blob_urls:
            relative_path = str(Path(key).relative_to(PFS_DIR))
            return {settings.DATA_UNDEFINED_NAME: f'{settings.HOSTNAME}/data/pfs/?d={relative_path}'}

        try:
            with Path(key).open(encoding='utf8') as f:
                value = json.load(f)
        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            raise ValueError(
                f"Can\'t import JSON-formatted tasks from {key}. If you're trying to import binary objects, "
                f"perhaps you've forgot to enable \"Treat every bucket object as a source file\" option?")

        if not isinstance(value, dict):
            raise ValueError(f"Error on key {key}: For {self.__class__.__name__} your JSON file must be a dictionary with one task.")  # noqa
        return value

    def scan_and_create_links(self):
        return self._scan_and_create_links(PachydermImportStorageLink)

    def sync(self):
        """Called when the "sync" button is clicked in the UI."""
        self.mount()
        self.scan_and_create_links()


class PachydermExportStorage(ExportStorage, PachydermMixin):

    def save_annotation(self, annotation):
        """
        Saves all annotations to files within the export mount.
        This method itself does not write the files to pachyderm.
        """
        if not self.is_mounted:
            raise RuntimeError(
                f"Output repository \"{self.repository_with_branch}\" not mounted\n"
                f"Please sync the associated target cloud storage"
            )

        logger.debug(f'Creating new object on {self.__class__.__name__} Storage {self} for annotation {annotation}')
        ser_annotation = self._get_serialized_data(annotation)

        # get key that identifies this object in storage
        key = PachydermExportStorageLink.get_key(annotation)
        key = os.path.join(self.mount_point, f"{key}.json")

        # put object into storage
        with open(key, mode='w') as f:
            json.dump(ser_annotation, f, indent=2)

        # Create export storage link
        PachydermExportStorageLink.create(annotation, self)

    def sync(self):
        """Called when the "sync" button is clicked in the UI."""
        if not self.is_mounted:
            self.mount(writable=True)
        self.save_all_annotations()
        self.unmount()
        self.mount(writable=True)


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


def split_branch(repository: str) -> Tuple[str, str]:
    """Split repository into (name, branch) at the @ symbol. """
    repo_name, _, branch = repository.partition("@")
    return repo_name, branch
