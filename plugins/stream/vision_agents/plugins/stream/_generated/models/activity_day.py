from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

T = TypeVar("T", bound="ActivityDay")


@_attrs_define
class ActivityDay:
    """
    Attributes:
        app_id (str): The app.
        area (str): The area of the API, such as agent_setup or conversations.
        client (str): Who sent the requests: dashboard, cli, sdk or api, the client each request named itself as, or
            router for work the router did for the app on its own, such as a scheduled task's run, a campaign's call, a
            simulation run or a forward to the app's endpoint.
        day (datetime.date): The UTC day, YYYY-MM-DD.
        first_at (datetime.datetime): When the first request arrived.
        last_at (datetime.datetime): When the last request arrived.
        last_refusal_code (str): The error code of the latest refusal, empty when there was none.
        organization_id (str): The organization the app was in when the row was last written.
        refused (int): How many were answered with a status of 400 or more.
        requests (int): How many requests arrived, refused ones included.
    """

    app_id: str
    area: str
    client: str
    day: datetime.date
    first_at: datetime.datetime
    last_at: datetime.datetime
    last_refusal_code: str
    organization_id: str
    refused: int
    requests: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        app_id = self.app_id

        area = self.area

        client = self.client

        day = self.day.isoformat()

        first_at = self.first_at.isoformat()

        last_at = self.last_at.isoformat()

        last_refusal_code = self.last_refusal_code

        organization_id = self.organization_id

        refused = self.refused

        requests = self.requests

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "app_id": app_id,
                "area": area,
                "client": client,
                "day": day,
                "first_at": first_at,
                "last_at": last_at,
                "last_refusal_code": last_refusal_code,
                "organization_id": organization_id,
                "refused": refused,
                "requests": requests,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        app_id = d.pop("app_id")

        area = d.pop("area")

        client = d.pop("client")

        day = datetime.date.fromisoformat(d.pop("day"))

        first_at = datetime.datetime.fromisoformat(d.pop("first_at"))

        last_at = datetime.datetime.fromisoformat(d.pop("last_at"))

        last_refusal_code = d.pop("last_refusal_code")

        organization_id = d.pop("organization_id")

        refused = d.pop("refused")

        requests = d.pop("requests")

        activity_day = cls(
            app_id=app_id,
            area=area,
            client=client,
            day=day,
            first_at=first_at,
            last_at=last_at,
            last_refusal_code=last_refusal_code,
            organization_id=organization_id,
            refused=refused,
            requests=requests,
        )

        activity_day.additional_properties = d
        return activity_day

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
