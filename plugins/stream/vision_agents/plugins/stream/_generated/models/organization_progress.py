from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..models.activity_step import ActivityStep

if TYPE_CHECKING:
    from ..models.activity_step_day import ActivityStepDay


T = TypeVar("T", bound="OrganizationProgress")


@_attrs_define
class OrganizationProgress:
    """
    Attributes:
        furthest_step (ActivityStep): The furthest an organization got: none, dashboard_reads, agent_setup, workers,
            conversations, voice_video, phone_sms, then steady_use, which is conversations or model calls answered on three
            UTC days or more. A step counts only answered requests: an area whose every request was refused was tried, not
            reached.
        organization_id (str): The organization.
        steps (list[ActivityStepDay]): Every step the organization's apps ever had a request answered in, whenever, not
            only from from to to, in furthest_step's order, with the day of the first, as getOrganizationActivity lists
            them.
    """

    furthest_step: ActivityStep
    organization_id: str
    steps: list[ActivityStepDay]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        furthest_step = self.furthest_step.value

        organization_id = self.organization_id

        steps = []
        for steps_item_data in self.steps:
            steps_item = steps_item_data.to_dict()
            steps.append(steps_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "furthest_step": furthest_step,
                "organization_id": organization_id,
                "steps": steps,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.activity_step_day import ActivityStepDay

        d = dict(src_dict)
        furthest_step = ActivityStep._decode(d.pop("furthest_step"))

        organization_id = d.pop("organization_id")

        steps = []
        _steps = d.pop("steps")
        for steps_item_data in _steps:
            steps_item = ActivityStepDay.from_dict(steps_item_data)

            steps.append(steps_item)

        organization_progress = cls(
            furthest_step=furthest_step,
            organization_id=organization_id,
            steps=steps,
        )

        organization_progress.additional_properties = d
        return organization_progress

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
