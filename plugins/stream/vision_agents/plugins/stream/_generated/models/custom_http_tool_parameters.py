from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

if TYPE_CHECKING:
    from ..models.custom_http_parameter import CustomHTTPParameter


T = TypeVar("T", bound="CustomHTTPToolParameters")


@_attrs_define
class CustomHTTPToolParameters:
    """The values the model gives, by name, up to 64. A parameter not in the path goes into the query of a GET, HEAD or
    DELETE and into a JSON body otherwise. A name a client reads as a method override, such as _method or X-HTTP-Method-
    Override, is refused.

    """

    additional_properties: dict[str, CustomHTTPParameter] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.custom_http_parameter import CustomHTTPParameter

        d = dict(src_dict)
        custom_http_tool_parameters = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = CustomHTTPParameter.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        custom_http_tool_parameters.additional_properties = additional_properties
        return custom_http_tool_parameters

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> CustomHTTPParameter:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: CustomHTTPParameter) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
