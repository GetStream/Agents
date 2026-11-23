from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.activity_day import ActivityDay
    from ..models.organization_progress import OrganizationProgress


T = TypeVar("T", bound="ActivityPage")


@_attrs_define
class ActivityPage:
    """
    Attributes:
        has_more (bool):
        items (list[ActivityDay]): One item per organization, day, app, area and client, sorted in that order.
        organizations (list[OrganizationProgress]): Every organization items names, by id. An organization whose items
            span two pages is on both, the same, unless a row was written between the two reads.
        next_cursor (str | Unset): Pass as cursor for the next page, with the same from and to. Absent on the last.
    """

    has_more: bool
    items: list[ActivityDay]
    organizations: list[OrganizationProgress]
    next_cursor: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        has_more = self.has_more

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        organizations = []
        for organizations_item_data in self.organizations:
            organizations_item = organizations_item_data.to_dict()
            organizations.append(organizations_item)

        next_cursor = self.next_cursor

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "has_more": has_more,
                "items": items,
                "organizations": organizations,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.activity_day import ActivityDay
        from ..models.organization_progress import OrganizationProgress

        d = dict(src_dict)
        has_more = d.pop("has_more")

        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = ActivityDay.from_dict(items_item_data)

            items.append(items_item)

        organizations = []
        _organizations = d.pop("organizations")
        for organizations_item_data in _organizations:
            organizations_item = OrganizationProgress.from_dict(organizations_item_data)

            organizations.append(organizations_item)

        next_cursor = d.pop("next_cursor", UNSET)

        activity_page = cls(
            has_more=has_more,
            items=items,
            organizations=organizations,
            next_cursor=next_cursor,
        )

        activity_page.additional_properties = d
        return activity_page

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
