from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from typing_extensions import Self

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.connector_client import ConnectorClient
    from ..models.custom_httpapi import CustomHTTPAPI


T = TypeVar("T", bound="CustomConnectorRequest")


@_attrs_define
class CustomConnectorRequest:
    """A custom MCP server or HTTP API for the app's agents to connect to. An unknown field is refused rather than ignored.

    Attributes:
        id (str): Starts with custom_, which no built-in does, so a custom definition never stands in for one. Creating
            an id that exists stores the next revision, unless the newest already says the same.
        name (str):
        schemes (list[str] | None): How a connection may authenticate. Each must be a scheme this deployment has, except
            oauth2_client_credentials, which a custom connector cannot use: it needs a token endpoint a custom connector
            cannot set.
        endpoint (str | Unset): The MCP server, over Streamable HTTP: a public https URL without userinfo, query or
            fragment. An address on a private network, loopback or link-local is refused. Set this or http, not both.
        category (str | Unset):
        client (ConnectorClient | Unset): How the OAuth client a connection uses is registered, and how the client
            authenticates at the token endpoint.
        description (str | Unset):
        scopes (list[str] | None | Unset): The scopes a consent asks for, each an RFC 6749 scope token.
        http (CustomHTTPAPI | Unset): An HTTP API a custom connector calls, and the tools it offers an agent.
        sync_hash (str | Unset): The fingerprint of the file this definition was synced from, which the connector then
            carries. Absent for a write that is no sync: the connector then carries none. Sending a definition the newest
            revision already says with another hash stores no new revision and records the hash on that one.
        check_changes (bool | Unset): Refuse the create, with unsynced_changes (409), when the newest revision of the id
            carries no sync_hash, because no sync wrote it, and says something else than this definition: a sync would write
            over an edit made outside it, in the dashboard, say. The same definition is not refused. Omitted, the create
            stores the next revision, as a sync that means to write over the edit wants.
    """

    id: str
    name: str
    schemes: list[str] | None
    endpoint: str | Unset = UNSET
    category: str | Unset = UNSET
    client: ConnectorClient | Unset = UNSET
    description: str | Unset = UNSET
    scopes: list[str] | None | Unset = UNSET
    http: CustomHTTPAPI | Unset = UNSET
    sync_hash: str | Unset = UNSET
    check_changes: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        schemes: list[str] | None
        if isinstance(self.schemes, list):
            schemes = self.schemes

        else:
            schemes = self.schemes

        endpoint = self.endpoint

        category = self.category

        client: dict[str, Any] | Unset = UNSET
        if not isinstance(self.client, Unset):
            client = self.client.to_dict()

        description = self.description

        scopes: list[str] | None | Unset
        if isinstance(self.scopes, Unset):
            scopes = UNSET
        elif isinstance(self.scopes, list):
            scopes = self.scopes

        else:
            scopes = self.scopes

        http: dict[str, Any] | Unset = UNSET
        if not isinstance(self.http, Unset):
            http = self.http.to_dict()

        sync_hash = self.sync_hash

        check_changes = self.check_changes

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "name": name,
                "schemes": schemes,
            }
        )
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
        if category is not UNSET:
            field_dict["category"] = category
        if client is not UNSET:
            field_dict["client"] = client
        if description is not UNSET:
            field_dict["description"] = description
        if scopes is not UNSET:
            field_dict["scopes"] = scopes
        if http is not UNSET:
            field_dict["http"] = http
        if sync_hash is not UNSET:
            field_dict["sync_hash"] = sync_hash
        if check_changes is not UNSET:
            field_dict["check_changes"] = check_changes

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.connector_client import ConnectorClient
        from ..models.custom_httpapi import CustomHTTPAPI

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        def _parse_schemes(data: object) -> list[str] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                schemes_type_0 = cast(list[str], data)

                return schemes_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None, data)

        schemes = _parse_schemes(d.pop("schemes"))

        endpoint = d.pop("endpoint", UNSET)

        category = d.pop("category", UNSET)

        _client = d.pop("client", UNSET)
        client: ConnectorClient | Unset
        if isinstance(_client, Unset):
            client = UNSET
        else:
            client = ConnectorClient.from_dict(_client)

        description = d.pop("description", UNSET)

        def _parse_scopes(data: object) -> list[str] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                scopes_type_0 = cast(list[str], data)

                return scopes_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None | Unset, data)

        scopes = _parse_scopes(d.pop("scopes", UNSET))

        _http = d.pop("http", UNSET)
        http: CustomHTTPAPI | Unset
        if isinstance(_http, Unset):
            http = UNSET
        else:
            http = CustomHTTPAPI.from_dict(_http)

        sync_hash = d.pop("sync_hash", UNSET)

        check_changes = d.pop("check_changes", UNSET)

        custom_connector_request = cls(
            id=id,
            name=name,
            schemes=schemes,
            endpoint=endpoint,
            category=category,
            client=client,
            description=description,
            scopes=scopes,
            http=http,
            sync_hash=sync_hash,
            check_changes=check_changes,
        )

        return custom_connector_request
