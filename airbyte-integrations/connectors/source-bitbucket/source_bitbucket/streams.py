#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import time
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib import parse

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from requests.exceptions import HTTPError
import json
from ratelimit import RateLimitException, limits, sleep_and_retry 
from backoff import on_exception, expo

ONE_HOUR = 3600

class BitbucketStream(HttpStream, ABC):
    url_base = "https://api.bitbucket.org/2.0/"

    primary_key = "id"
    use_cache = True

    # Bitbucket pagination could be from 1 to 100.
    page_size = 50

    stream_base_params = {}

    def __init__(self, repositories: List[str], **kwargs):
        super().__init__(**kwargs)
        self.repositories = repositories

        MAX_RETRIES = 3
        adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"repositories/{stream_slice['repository']}/{self.name}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for repository in self.repositories:
            yield {"repository": repository}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        
        content = json.loads(response.content)

        if "next" in content:
            next_link = content["next"]
            parsed_link = parse.urlparse(next_link)
            page = dict(parse.parse_qsl(parsed_link.query)).get("page")
            if page == None:
                return None
            return {"page": page}

    def should_retry(self, response: requests.Response) -> bool:
        # We don't call `super()` here because we have custom error handling and Bitbucket API sometimes returns strange
        # errors. So in `read_records()` we have custom error handling which don't require to call `super()` here.
        should_retry = super().should_retry(response)
        if should_retry == False:
            pass
        return response.status_code in (
            requests.codes.SERVER_ERROR,
            requests.codes.BAD_GATEWAY, 429, 401, 403, 404, 500, 502, 503, 504
        )

    def backoff_time(self, response: requests.Response) -> Union[int, float]:
        # This method is called if we run into the rate limit. Bitbucket limits requests to 1000 per hour and provides
        if response.status_code != 429:
            return None
        return None  # This is a guarantee that no negative value will be returned.

    def call_counter(func):
        def helper(*args, **kwargs):
            helper.calls += 1
            if helper.calls == 990:
                print("About to the exceed the 1000 requests limit per hour, sleeping for 61 seconds")
                time.sleep(61)
                helper.calls = 0
            return func(*args, **kwargs)
        helper.calls = 0
        helper.__name__= func.__name__
        return helper

    @call_counter
    def read_records(self, stream_slice: Mapping[str, any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            self.logger.info(f"Reading records from Bitbucket {stream_slice.get('repository', {})}")
            yield from super().read_records(stream_slice=stream_slice, **kwargs)
        except HTTPError as e:
            # This error occours due to a bug in the diffstats endpoint described in https://jira.atlassian.com/browse/BCLOUD-20247
            if e.response.status_code == 555:
                self.logger.error(f"The following error occurred: {e}. Make the commit for the pull request smaller or remove it.")
                return {}
            if e.response.status_code == 404:
                self.logger.error(f"The following error occurred: {e}. Make the commit for the pull request smaller or remove it.")
                return {}
            if e.response.status_code == 401:
                self.logger.error(f"The following error occurred: {e}. Make the commit for the pull request smaller or remove it.")
                return {}

            error_msg = str(e)
            self.logger.error(f"Error Message: {error_msg}")
            raise e

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {"per_page": self.page_size}

        if next_page_token:
            params.update(next_page_token)

        params.update(self.stream_base_params)

        return params

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        # Without sending `User-Agent` header we will be getting `403 Client Error: Forbidden for url` error.
        return {
            "User-Agent": "PostmanRuntime/7.28.0",
        }

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        for record in response.json()["values"]:  # Bitbucket puts records in an array.
            yield self.transform(record=record, repository=stream_slice["repository"])

    def transform(self, record: MutableMapping[str, Any], repository: str = None, workspace: str = None) -> MutableMapping[str, Any]:
        if repository:
            record["repository"] = repository
        if workspace:
            record["workspace"] = workspace

        return record

class SemiIncrementalBitbucketStream(BitbucketStream):
    """
    Semi incremental streams are also incremental but with one difference, they:
      - read all records;
      - output only new records.
    This means that semi incremental streams read all records (like full_refresh streams) but do filtering directly
    in the code and output only latest records (like incremental streams).
    """

    cursor_field = "updated_on"

    # This flag is used to indicate that current stream supports `sort` and `direction` request parameters and that
    # we should break processing records if possible. If `sort` is set to `updated` and `direction` is set to `desc`
    # this means that latest records will be at the beginning of the response and after we processed those latest
    # records we can just stop and not process other record. This will increase speed of each incremental stream
    # which supports those 2 request parameters. Currently only `IssueMilestones` and `PullRequests` streams are
    # supporting this.
    is_sorted_descending = False

    def __init__(self, start_date: str, **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date

    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        if not self.is_sorted_descending:
            return self.page_size
        return None

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]):
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state
        object and returning an updated state object.
        """
        state_value = latest_cursor_value = latest_record.get(self.cursor_field)
        current_repository = latest_record["repository"]

        if current_stream_state.get(current_repository, {}).get(self.cursor_field):
            state_value = max(latest_cursor_value, current_stream_state[current_repository][self.cursor_field])
        current_stream_state[current_repository] = {self.cursor_field: state_value}
        return current_stream_state

    def get_starting_point(self, stream_state: Mapping[str, Any], repository: str) -> str:
        start_point = self._start_date

        if stream_state and stream_state.get(repository, {}).get(self.cursor_field):
            start_point = max(start_point, stream_state[repository][self.cursor_field])

        return start_point

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        start_point_map = {repo: self.get_starting_point(stream_state=stream_state, repository=repo) for repo in self.repositories}
        for record in super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        ):
            if record.get(self.cursor_field) > start_point_map[stream_slice["repository"]]:
                yield record
            elif self.is_sorted_descending and record.get(self.cursor_field) < start_point_map[stream_slice["repository"]]:
                break

class IncrementalBitbucketStream(SemiIncrementalBitbucketStream):
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, **kwargs)
        since_params = self.get_starting_point(stream_state=stream_state, repository=stream_slice["repository"])
        if since_params:
            params["since"] = since_params
        return params


class Branches(BitbucketStream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/repos#list-branches
    """

    primary_key = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"repositories/{stream_slice['repository']}/refs/branches"


class Workspaces(BitbucketStream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/orgs#get-an-workspace
    """

    def __init__(self, workspaces: List[str], **kwargs):
        super(BitbucketStream, self).__init__(**kwargs)
        self.workspaces = workspaces

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for workspace in self.workspaces:
            yield {"workspace": workspace}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"workspaces/{stream_slice['workspace']}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()

class Repositories(Workspaces):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/repos#list-workspace-repositories
    """

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"repositories/{stream_slice['workspace']}"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        for record in response.json()["values"]:  # Bitbucket puts records in an array.
            yield self.transform(record=record, workspace=stream_slice["workspace"])


class PullRequests(SemiIncrementalBitbucketStream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/pulls#list-pull-requests
    """

    page_size = 50
    first_read_override_key = "first_read_override"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._first_read = True

    def read_records(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Decide if this a first read or not by the presence of the state object
        """
        self._first_read = not bool(stream_state) or stream_state.get(self.first_read_override_key, False)
        yield from super().read_records(stream_state=stream_state, **kwargs)

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"repositories/{stream_slice['repository']}/pullrequests"

    def transform(self, record: MutableMapping[str, Any], repository: str = None, **kwargs) -> MutableMapping[str, Any]:
        record = super().transform(record=record, repository=repository, **kwargs)
        record["repo_id"] = record.get("source_repository_uuid")
        
        return record

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        base_params = super().request_params(**kwargs)
        # The very first time we read this stream we want to read ascending so we can save state in case of
        # a halfway failure. But if there is state, we read descending to allow incremental behavior.
        params = {"state": "all", "sort": "updated_on", "direction": "desc" if self.is_sorted_descending else "asc"}

        return {**base_params, **params}

    @property
    def is_sorted_descending(self) -> bool:
        """
        Depending if there any state we read stream in ascending or descending order.
        """
        return not self._first_read


class PullRequestSubstream(HttpSubStream, SemiIncrementalBitbucketStream, ABC):
    use_cache = False

    def __init__(self, parent: PullRequests, **kwargs):
        super().__init__(parent=parent, **kwargs)

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Override the parent PullRequests stream configuration to always fetch records in ascending order
        """
        parent_state = deepcopy(stream_state) or {}
        parent_state[PullRequests.first_read_override_key] = True
        parent_stream_slices = super().stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=parent_state)
        for parent_stream_slice in parent_stream_slices:
            if parent_stream_slice["parent"]["state"] != "DECLINED":
                yield {
                    "pull_request_id": parent_stream_slice["parent"]["id"],
                    "repository": parent_stream_slice["parent"]["repository"],
                    "pull_request_state": parent_stream_slice["parent"]["state"],
                }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        We've already determined the list of pull requests to run the stream against.
        Skip the start_point_map and cursor_field logic in SemiIncrementalBitbucketStream.read_records.
        """
        yield from super(SemiIncrementalBitbucketStream, self).read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        )
    def transform(self, record: MutableMapping[str, Any], repository: str = None, pull_request_id: str = None, **kwargs) -> MutableMapping[str, Any]:
        record = super().transform(record=record, repository=repository, **kwargs)
        record['pull_request_id'] = pull_request_id
        return record


# repositories > {workspace} > {repo_slug} > pullrequests > {pull_request_id} > diff
class PullRequestDiff(PullRequestSubstream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/pulls#get-a-pull-request
    """

    @property
    def record_keys(self) -> List[str]:
        return list(self.get_json_schema()["properties"].keys())

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"repositories/{stream_slice['repository']}/pullrequests/{stream_slice['pull_request_id']}/diffstat"

    # For certain pull requests with too many commits, diffstat takes too long to return.

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        if stream_slice["pull_request_state"] != "DECLINED":
            for record in response.json()["values"]:  # Bitbucket puts records in an array.
                yield self.transform(record, repository=stream_slice["repository"], pull_request_id=stream_slice["pull_request_id"])
        # The endoint returns a string, so we need to parse it into a dict.

    def transform(self, record: MutableMapping[str, Any], repository: str = None, pull_request_id: str = None) -> MutableMapping[str, Any]:
        record = super().transform(record=record, repository=repository, pull_request_id=pull_request_id)
        return record

# repositories > {workspace} > {repo_slug} > pullrequests > {pull_request_id} > Comments
class PullRequestComments(PullRequestSubstream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/pulls#get-a-pull-request
    """

    @property
    def record_keys(self) -> List[str]:
        return list(self.get_json_schema()["properties"].keys())

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"repositories/{stream_slice['repository']}/pullrequests/{stream_slice['pull_request_id']}/comments"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        for record in response.json()["values"]:  # GitHub puts records in an array.
            yield self.transform(record=record, repository=stream_slice["repository"], \
                                    pull_request_id=stream_slice["pull_request_id"])

    def transform(self, record: MutableMapping[str, Any], repository: str = None, pull_request_id: str = None) -> MutableMapping[str, Any]:
        record = super().transform(record=record, repository=repository, pull_request_id=pull_request_id)
        return record

# repositories > {workspace} > {repo_slug} > pullrequests > {pull_request_id} > Activities
class PullRequestActivity(PullRequestSubstream):
    """
    API docs: https://docs.Bitbucket.com/en/rest/reference/pulls#get-a-pull-request
    """

    @property
    def record_keys(self) -> List[str]:
        return list(self.get_json_schema()["properties"].keys())

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"repositories/{stream_slice['repository']}/pullrequests/{stream_slice['pull_request_id']}/activity"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any], **kwargs) -> Iterable[Mapping]:
        for record in response.json()["values"]:
            yield self.transform(record=record, repository=stream_slice["repository"], pull_request_id=stream_slice["pull_request_id"])

    def transform(self, record: MutableMapping[str, Any], repository: str = None, pull_request_id: str = None) -> MutableMapping[str, Any]:
        record = {key: value for key, value in record.items() if key in self.record_keys}
        record = super().transform(record=record, repository=repository, pull_request_id=pull_request_id)
        return record

class RepositoryStats(BitbucketStream):
    """
    This stream is technical and not intended for the user, we use it for checking connection with the repository.
    API docs: https://docs.github.com/en/rest/reference/repos#get-a-repository
    """

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"repositories/{stream_slice['repository']}"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        yield response.json()
