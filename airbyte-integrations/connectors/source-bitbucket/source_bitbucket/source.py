#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import re
from typing import Any, Dict, List, Mapping, Tuple
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth.oauth import Oauth2Authenticator

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

TOKEN_SEPARATOR = ","
# To scan all the repos within orgnaization, workspace name could be
# specified by using asteriks i.e. "airbytehq/*"
workspace_PATTERN = re.compile("^.*/\\*$")

class SourceBitbucket(AbstractSource):
    @staticmethod
    def _generate_repositories(config: Mapping[str, Any], authenticator: Oauth2Authenticator) -> Tuple[List[str], List[str]]:
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
        
        creds = config.get("credentials")
        if creds != None:
            client_id = creds.get("client_id")
            client_secret = creds.get("client_secret")
            refresh_token = creds.get("refresh_token")
        else:
            client_id = config.get("client_id")
            client_secret = config.get("client_secret")
            refresh_token = config.get("refresh_token")

        return Oauth2Authenticator(token_refresh_endpoint="https://bitbucket.org/site/oauth2/access_token", client_id=client_id, client_secret=client_secret, refresh_token=refresh_token)


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