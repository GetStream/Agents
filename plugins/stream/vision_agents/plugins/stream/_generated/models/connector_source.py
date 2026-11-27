from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from typing_extensions import Self

from ..models.connector_source_kind import ConnectorSourceKind

T = TypeVar("T", bound="ConnectorSource")


@_attrs_define
class ConnectorSource:
    """One way a connector's tools are reached. Only its kind is shown.

    Attributes:
        kind (ConnectorSourceKind): mcp is an MCP server that lists the tools, http an HTTP API whose tools the manifest
            lists.
    """

    kind: ConnectorSourceKind

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        kind = ConnectorSourceKind._decode(d.pop("kind"))

        connector_source = cls(
            kind=kind,
        )

        return connector_source
