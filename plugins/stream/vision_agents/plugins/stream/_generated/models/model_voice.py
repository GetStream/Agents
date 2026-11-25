from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelVoice")


@_attrs_define
class ModelVoice:
    """A built-in voice a native speech model accepts by id.

    Attributes:
        id (str): The voice identifier the provider accepts in its voice field. Example: marin.
        name (str): Display name for the voice. Example: Marin.
        default (bool | Unset): True when this is the model's default voice.
        description (str | Unset): Short description of the voice's tone, style or accent.
        gender (str | Unset): Voice presentation, such as female, male or neutral.
    """

    id: str
    name: str
    default: bool | Unset = UNSET
    description: str | Unset = UNSET
    gender: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        default = self.default

        description = self.description

        gender = self.gender

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
            }
        )
        if default is not UNSET:
            field_dict["default"] = default
        if description is not UNSET:
            field_dict["description"] = description
        if gender is not UNSET:
            field_dict["gender"] = gender

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        default = d.pop("default", UNSET)

        description = d.pop("description", UNSET)

        gender = d.pop("gender", UNSET)

        model_voice = cls(
            id=id,
            name=name,
            default=default,
            description=description,
            gender=gender,
        )

        model_voice.additional_properties = d
        return model_voice

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
