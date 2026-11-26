from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from typing_extensions import Self

if TYPE_CHECKING:
    from ..models.custom_http_tool import CustomHTTPTool


T = TypeVar("T", bound="CustomHTTPAPI")


@_attrs_define
class CustomHTTPAPI:
    """An HTTP API a custom connector calls, and the tools it offers an agent.

    Attributes:
        base_url (str): The API the tools call, each at its path under it: a public https URL without userinfo, query or
            fragment. An address on a private network, loopback or link-local is refused. Never shown once stored.
        tools (list[CustomHTTPTool]): At least one and at most 128, each name once.
    """

    base_url: str
    tools: list[CustomHTTPTool]

    def to_dict(self) -> dict[str, Any]:
        base_url = self.base_url

        tools = []
        for tools_item_data in self.tools:
            tools_item = tools_item_data.to_dict()
            tools.append(tools_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "base_url": base_url,
                "tools": tools,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.custom_http_tool import CustomHTTPTool

        d = dict(src_dict)
        base_url = d.pop("base_url")

        tools = []
        _tools = d.pop("tools")
        for tools_item_data in _tools:
            tools_item = CustomHTTPTool.from_dict(tools_item_data)

            tools.append(tools_item)

        custom_httpapi = cls(
            base_url=base_url,
            tools=tools,
        )

        return custom_httpapi
