from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..models.routing_policy import RoutingPolicy
from ..types import UNSET, Unset

T = TypeVar("T", bound="UpdateAppSettings")


@_attrs_define
class UpdateAppSettings:
    """
    Attributes:
        routing_policy (RoutingPolicy | Unset): Which regions work may be placed in. closest is the region nearest the
            person: where a request landed, the called number's country, or the region that last served the user. us_east is
            the region holding the configs, for everything. us_only, eu_only and canada_only place the work (sessions,
            calls, runs, reminders, dials and dispatch workers) in that geography, and fail it (503 shard_unavailable)
            rather than place it anywhere else when none of its regions is up. Some data is still stored in the region
            holding the configs whatever the policy: configs, the global index of sessions, inbound message bodies, task
            instructions, knowledge and contacts. Empty is the app's own on an agent config, and closest on the app.
    """

    routing_policy: RoutingPolicy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        routing_policy: str | Unset = UNSET
        if not isinstance(self.routing_policy, Unset):
            routing_policy = self.routing_policy.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if routing_policy is not UNSET:
            field_dict["routing_policy"] = routing_policy

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        _routing_policy = d.pop("routing_policy", UNSET)
        routing_policy: RoutingPolicy | Unset
        if isinstance(_routing_policy, Unset):
            routing_policy = UNSET
        else:
            routing_policy = RoutingPolicy._decode(_routing_policy)

        update_app_settings = cls(
            routing_policy=routing_policy,
        )

        update_app_settings.additional_properties = d
        return update_app_settings

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
