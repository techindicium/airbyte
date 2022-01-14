#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, Iterable, Mapping, MutableMapping

from airbyte_cdk.connector import Connector
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, ConfiguredAirbyteCatalog


class Source(Connector, ABC):
    # can be overridden to change an input state
    def read_state(self, state_path: str) -> Dict[str, Any]:
        if state_path:
            state_obj = json.loads(open(state_path, "r").read())
        else:
            state_obj = {}
        state = defaultdict(dict, state_obj)
        return state

    # can be overridden to change an input catalog
    def read_catalog(self, catalog_path: str) -> ConfiguredAirbyteCatalog:
        return ConfiguredAirbyteCatalog.parse_obj(self.read_config(catalog_path))

    @abstractmethod
    def read(
        self, logger: logging.Logger, config: Mapping[str, Any], catalog: ConfiguredAirbyteCatalog, state: MutableMapping[str, Any] = None
    ) -> Iterable[AirbyteMessage]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration, catalog, and state.
        """

    @abstractmethod
    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration. For example, given valid credentials to a
        Postgres database, returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.
        """
