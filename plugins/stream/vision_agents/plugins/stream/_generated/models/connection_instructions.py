from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

T = TypeVar("T", bound="ConnectionInstructions")


@_attrs_define
class ConnectionInstructions:
    """
    Attributes:
        digest (str): The SHA-256 of the text. When the server changes its instructions, validate reports
            needs_reapproval with (instructions) in changed_tools. Approve the new text with POST .../tools/approve, sending
            the name (instructions) and this digest.
        text (str): The instructions, as the server sent them, without leading or trailing whitespace. Empty when it
            sent none.
    """

    digest: str
    text: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        digest = self.digest

        text = self.text

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "digest": digest,
                "text": text,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        digest = d.pop("digest")

        text = d.pop("text")

        connection_instructions = cls(
            digest=digest,
            text=text,
        )

        connection_instructions.additional_properties = d
        return connection_instructions

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
