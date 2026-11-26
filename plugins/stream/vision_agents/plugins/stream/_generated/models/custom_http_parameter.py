from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from typing_extensions import Self

from ..models.custom_http_parameter_type import CustomHTTPParameterType
from ..types import UNSET, Unset

T = TypeVar("T", bound="CustomHTTPParameter")


@_attrs_define
class CustomHTTPParameter:
    """One value the model gives an http tool.

    Attributes:
        type_ (CustomHTTPParameterType):
        description (str | Unset):
        required (bool | Unset): The model must give it. A parameter in the path must be required.
    """

    type_: CustomHTTPParameterType
    description: str | Unset = UNSET
    required: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        description = self.description

        required = self.required

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if required is not UNSET:
            field_dict["required"] = required

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        type_ = CustomHTTPParameterType._decode(d.pop("type"))

        description = d.pop("description", UNSET)

        required = d.pop("required", UNSET)

        custom_http_parameter = cls(
            type_=type_,
            description=description,
            required=required,
        )

        return custom_http_parameter
