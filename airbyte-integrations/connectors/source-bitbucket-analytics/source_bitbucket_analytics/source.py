#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import re
from typing import Any, Dict, List, Mapping, Tuple, MutableMapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import MultipleTokenAuthenticator
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator
from requests.auth import AuthBase
import json
import requests

from .streams import (
    Branches,
    Workspaces,
    PullRequests,
    PullRequestDiff,
    PullRequestComments,
    PullRequestActivity,
    Repositories,
    RepositoryStats,
)

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""

TOKEN_SEPARATOR = ","
# To scan all the repos within orgnaization, workspace name could be
# specified by using asteriks i.e. "airbytehq/*"
workspace_PATTERN = re.compile("^.*/\\*$")

class BitbucketAuthenticator(AuthBase):
    """
    Temporary method to get access token
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        access_token_name: str = "access_token",
    ):
        self.client_secret = client_secret
        self.client_id = client_id
        self.access_token_name = access_token_name
        self.access_token_endpoint = "https://bitbucket.org/site/oauth2/access_token"
        self._access_token = None


    def __call__(self, request):
        request.headers.update(self.get_auth_header())
        return request

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    def get_access_token(self):
        return self.access_token()

    def get_request_body(self) -> Mapping[str, Any]:
        """Override to define additional parameters"""
        payload: MutableMapping[str, Any] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        return payload

    def access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(method="POST", url=self.access_token_endpoint, data=self.get_request_body())
            response.raise_for_status()
            access_token = json.loads(response.content)["access_token"]            
            return access_token
        except Exception as e:
            raise Exception(f"Error while getting the access token: {e}") from e


class SourceBitbucketAnalytics(AbstractSource):
    @staticmethod
    def _generate_repositories(config: Mapping[str, Any], authenticator: BitbucketAuthenticator) -> Tuple[List[str], List[str]]:
        """
        Parse repositories config line and produce two lists of repositories.
        Args:
            config (dict): Dict representing connector's config
            authenticator(MultipleTokenAuthenticator): authenticator object
        Returns:
            Tuple[List[str], List[str]]: Tuple of two lists: first representing
            repositories directly mentioned in config and second is
            workspace repositories from orgs/{org}/repos request.
        """
        repositories = list(filter(None, config["repository"].split(" ")))

        if not repositories:
            raise Exception("Field `repository` required to be provided for connect to Bitbucket API")

        repositories_list: set = {repo for repo in repositories if not workspace_PATTERN.match(repo)}
        workspaces = [org.split("/")[0] for org in repositories if org not in repositories_list]
        organisation_repos = set()
        if workspaces:
            repos = Repositories(authenticator=authenticator, workspaces=workspaces)
            for stream in repos.stream_slices(sync_mode=SyncMode.full_refresh):
                organisation_repos = organisation_repos.union(
                    {r["full_name"] for r in repos.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream)}
                )

        return list(repositories_list), list(organisation_repos)

    @staticmethod
    def _get_authenticator(config: Dict[str, Any]):
        # Before we supported oauth, personal_access_token was called `access_token` and it lived at the
        # config root. So we first check to make sure any backwards compatbility is handled.
        token = config.get("access_token")
        if not token:
            creds = config.get("credentials")
            token = creds.get("access_token") or creds.get("personal_access_token")
        tokens = [t.strip() for t in token.split(TOKEN_SEPARATOR)]

        client_id = config.get("client_id")
        client_secret = config.get("client_secret")


        # return Oauth2Authenticator(client_id=client_id, client_secret=client_secret)
        # return MultipleTokenAuthenticator(tokens=tokens, auth_method="Bearer")
        return BitbucketAuthenticator(client_id=client_id, client_secret=client_secret)


    @staticmethod
    def _get_branches_data(selected_branches: str, full_refresh_args: Dict[str, Any] = None) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
        selected_branches = set(filter(None, selected_branches.split(" ")))

        # Get the default branch for each repository
        default_branches = {}
        repository_stats_stream = RepositoryStats(**full_refresh_args)
        for stream_slice in repository_stats_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            default_branches.update(
                {
                    repo_stats["full_name"]: repo_stats["mainbranch"]["name"]
                    for repo_stats in repository_stats_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)
                }
            )

        all_branches = []
        branches_stream = Branches(**full_refresh_args)
        for stream_slice in branches_stream.stream_slices(sync_mode=SyncMode.full_refresh):
            for branch in branches_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice):
                all_branches.append(f"{branch['repository']}/{branch['name']}")

        # Create mapping of repository to list of branches to pull commits for
        # If no branches are specified for a repo, use its default branch
        branches_to_pull: Dict[str, List[str]] = {}
        for repo in full_refresh_args["repositories"]:
            repo_branches = []
            for branch in selected_branches:
                branch_parts = branch.split("/", 2)
                if "/".join(branch_parts[:2]) == repo and branch in all_branches:
                    repo_branches.append(branch_parts[-1])
            if not repo_branches:
                repo_branches = [default_branches[repo]]

            branches_to_pull[repo] = repo_branches

        return default_branches, branches_to_pull

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            authenticator = self._get_authenticator(config)
            # In case of getting repository list for given workspace was
            # successfull no need of checking stats for every repository within
            # that workspace.
            # Since we have "repo" scope requested it should grant access to private repos as well:
            # https://docs.github.com/en/developers/apps/building-oauth-apps/scopes-for-oauth-apps#available-scopes
            repositories, _ = self._generate_repositories(config=config, authenticator=authenticator)

            repository_stats_stream = RepositoryStats(
                authenticator=authenticator,
                repositories=repositories,
            )
            for stream_slice in repository_stats_stream.stream_slices(sync_mode=SyncMode.full_refresh):
                next(repository_stats_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice), None)
            return True, None
        except Exception as e:
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self._get_authenticator(config)
        repos, workspace_repos = self._generate_repositories(config=config, authenticator=authenticator)
        repositories = repos + workspace_repos

        workspaces = list({org.split("/")[0] for org in repositories})

        workspace_args = {"authenticator": authenticator, "workspaces": workspaces}
        repository_args = {"authenticator": authenticator, "repositories": repositories}
        repository_args_with_start_date = {**repository_args, "start_date": config["start_date"]}

        # default_branches, branches_to_pull = self._get_branches_data(config.get("branch", ""), repository_args)
        pull_requests_stream = PullRequests(**repository_args_with_start_date)

        return [
            Workspaces(**workspace_args),
            PullRequestDiff(parent=pull_requests_stream, **repository_args_with_start_date),
            PullRequestActivity(parent=pull_requests_stream, **repository_args_with_start_date),
            PullRequestComments(parent=pull_requests_stream, **repository_args_with_start_date),
            PullRequests(**repository_args_with_start_date),
            Repositories(**workspace_args)
        ]