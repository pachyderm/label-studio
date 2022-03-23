"""This file and its contents are licensed under the Apache License 2.0. Please see the included NOTICE for copyright information and LICENSE for a copy of the license.
"""
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen
from time import sleep
from typing import Dict, Tuple

from django.conf import settings
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from rest_framework.exceptions import ValidationError
from requests import get, put, RequestException

from io_storages.base_models import (
      ExportStorage,
      ExportStorageLink,
      ImportStorage,
      ImportStorageLink,
)
from tasks.models import Annotation

MOUNT_SERVER_URL = "http://localhost:9002"
PFS_DIR = Path("/pfs")
logger = logging.getLogger(__name__)

_mount_process = None
_mounts: Dict[int, str] = dict()


class PachydermMixin(models.Model):
    repository = models.TextField(_('repository'), blank=True, help_text='Local path')

    @property
    def is_mounted(self) -> bool:
        # Maybe we should do something with the stored process here.
        return self.mount_point.exists()

    @property
    def mount_point(self) -> Path:
        return PFS_DIR / str(self.repository_with_branch)

    @property
    def repository_with_branch(self) -> str:
        repo_name, branch = split_branch(str(self.repository))
        branch = branch or "master"
        return f"{repo_name}@{branch}"

    @staticmethod
    def get_repos() -> Dict[str, "Repo"]:
        response = get(f"{MOUNT_SERVER_URL}/repos")
        response.raise_for_status()
        return {
            name: Repo.from_dict(repo)
            for name, repo in response.json().items()
        }

    @classmethod
    def safe_start_mount_server(cls) -> None:
        try:
            cls.get_repos()
        except:
            global _mount_process
            _mount_process = Popen(["pachctl", "mount-server"])
            sleep(1)

    def mount(self, wait: int = 30, *, writable: bool = False) -> None:
        self.safe_start_mount_server()
        repository_with_branch = str(self.repository_with_branch)
        repo_name, branch = split_branch(repository_with_branch)
        mounted_repo = _mounts.get(self.pk, None)
        if mounted_repo is not None and mounted_repo != repository_with_branch:
            self.unmount(mounted_repo)

        logger.debug(f"Mounting repository: {repository_with_branch}")
        if not self.is_mounted:
            put(
                url=f"{MOUNT_SERVER_URL}/repos/{repo_name}/{branch}/_mount",
                params=dict(
                    name=repository_with_branch,
                    mode="rw" if writable else "r",
                ),
            ).raise_for_status()
            _mounts[self.pk] = str(repository_with_branch)
            for _ in range(wait):
                if self.is_mounted:
                    break
                sleep(1)

    def unmount(self, repository: str) -> None:
        self.safe_start_mount_server()
        repo_name, branch = split_branch(repository)
        logger.debug(f"Unmounting repository: {repository}")
        put(
            url=f"{MOUNT_SERVER_URL}/repos/{repo_name}/{branch}/_unmount",
            params=dict(name=repository)
        ).raise_for_status()
        del _mounts[self.pk]

    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)
        if self.is_mounted:
            self.unmount(str(self.repository_with_branch))

    def validate_connection(self):
        self.safe_start_mount_server()
        if not PFS_DIR.is_dir():
            raise ValidationError(f"Mount directory {PFS_DIR} does not exist.")
        self.clean()
        repo_name, branch = split_branch(str(self.repository_with_branch))
        repositories = self.get_repos()
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
        for file in self.mount_point.rglob('*'):
            if file.is_file():
                yield str(file)

    def get_data(self, key):
        relative_path = str(Path(key).relative_to(PFS_DIR))
        return {settings.DATA_UNDEFINED_NAME: f'{settings.HOSTNAME}/data/pfs/?d={relative_path}'}

    def scan_and_create_links(self):
        return self._scan_and_create_links(PachydermImportStorageLink)

    def sync(self):
        self.mount()
        self.scan_and_create_links()


class PachydermExportStorage(ExportStorage, PachydermMixin):

    def save_annotation(self, annotation):
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
        if not self.is_mounted:
            self.mount(writable=True)
        self.save_all_annotations()
        self.unmount(self.repository_with_branch)
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
    repo_name, _, branch = repository.partition("@")
    return repo_name, branch


@dataclass
class Mount:
    name: str
    mode: str
    state: str
    status: str
    mountpoint: str

    @classmethod
    def from_dict(cls, data: Dict) -> "Mount":
        return cls(
            name=data['name'],
            mode=data['mode'],
            state=data['state'],
            status=data['status'],
            mountpoint=data['mountpoint'],
        )


@dataclass
class Branch:
    name: str
    mount: Mount

    @classmethod
    def from_dict(cls, data: Dict) -> "Branch":
        return cls(
            name=data['name'],
            mount=Mount.from_dict(data['mount']),
        )


@dataclass
class Repo:
    name: str
    branches: Dict[str, Branch]

    @classmethod
    def from_dict(cls, data: Dict) -> "Repo":
        return cls(
            name=data['name'],
            branches={
                name: Branch.from_dict(branch)
                for name, branch in data['branches'].items()
            },
        )
