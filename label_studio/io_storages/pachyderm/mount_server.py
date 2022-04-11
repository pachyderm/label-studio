"""Functionality for interacting with the pachctl mount-server."""
import logging
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from subprocess import Popen
from time import sleep
from typing import Dict, Literal, Optional

import requests
from requests import get, put, RequestException

HEADERS = {'Accept': '*/*', 'Accept-Encoding': ''}
MOUNT_MODE = Literal["r", "rw"]
MOUNT_SERVER_URL = "http://localhost:9002"
_mount_process: Optional[Popen] = None
logger = logging.getLogger(__name__)


@dataclass
class Mount:
    """Mount object from the mount-server response."""
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
    """Branch object from the mount-server response."""
    name: str
    mount: Mount

    @classmethod
    def from_dict(cls, data: Dict) -> "Branch":
        return cls(
            name=data['name'],
            mount=Mount.from_dict(data['mount'][0]),
        )


@dataclass
class Repo:
    """Repo object from the mount-server response."""
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


def get_repos() -> Dict[str, "Repo"]:
    """Returns the deserialized response from GET /repos"""
    response = get(f"{MOUNT_SERVER_URL}/repos")
    response.raise_for_status()
    return {
        name: Repo.from_dict(repo)
        for name, repo in response.json().items()
    }


def mount_repo(
    repo: str, branch: str, mode: MOUNT_MODE = 'r', name: Optional[str] = None
) -> str:
    """Mount the specified branch of the specified pachyderm repository"""
    name = name or f"{repo}@{branch}"
    url = f"{MOUNT_SERVER_URL}/repos/{repo}/{branch}/_mount"
    params = urllib.parse.urlencode(dict(name=name, mode=mode), safe='@')
    response = requests.put(url, params=params)
    logger.warning(list(Path(f'/pfs/{name}').glob("*")))
    logger.warning(response.request.url)
    logger.warning(response.request.headers)
    logger.warning(response.text)
    response.raise_for_status()

    return name


def unmount_repo(repo: str, branch: str, name: Optional[str] = None) -> None:
    """Unmount the specified branch of the specified pachyderm repository"""
    name = name or f"{repo}@{branch}"
    url = f"{MOUNT_SERVER_URL}/repos/{repo}/{branch}/_unmount"
    put(url, params=dict(name=name)).raise_for_status()


def safe_start_mount_server(*, wait: int = 30) -> None:
    """Start the mount-server if it is not already started.

    This uses GET /repos to check if the mount-server is started which
    is the current best option but scales poorly with # of repositories.
    """
    try:
        get_repos()
    except RequestException:
        global _mount_process
        _mount_process = Popen(["pachctl", "mount-server"])
        for _ in range(wait):
            try:
                get_repos()
                return
            except RequestException:
                sleep(1)
        raise TimeoutError("Failed to start pachctl mount-server")
