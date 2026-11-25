from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..models.routing_policy import RoutingPolicy

if TYPE_CHECKING:
    from ..models.stream_settings import StreamSettings


T = TypeVar("T", bound="AppSettings")


@_attrs_define
class AppSettings:
    """What the router does for the calling app. It never carries a secret.

    Attributes:
        stream (StreamSettings): Which Stream app the router writes the calling app's conversations, transcripts, calls
            and phone lines into, and whether that app holds the types they need.
        routing_policy (RoutingPolicy): Which regions work may be placed in. closest is the region nearest the person:
            where a request landed, the called number's country, or the region that last served the user. us_east is the
            region holding the configs, for everything. us_only, eu_only and canada_only keep all of the work and its data
            in that geography, and fail it (503 shard_unavailable) rather than place it anywhere else when none of its
            regions is up. Empty is the app's own on an agent config, and closest on the app.
    """

    stream: StreamSettings
    routing_policy: RoutingPolicy
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        stream = self.stream.to_dict()

        routing_policy = self.routing_policy.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "stream": stream,
                "routing_policy": routing_policy,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.stream_settings import StreamSettings

        d = dict(src_dict)
        stream = StreamSettings.from_dict(d.pop("stream"))

        routing_policy = RoutingPolicy._decode(d.pop("routing_policy"))

        app_settings = cls(
            stream=stream,
            routing_policy=routing_policy,
        )

        app_settings.additional_properties = d
        return app_settings

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
