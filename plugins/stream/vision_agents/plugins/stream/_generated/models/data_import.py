from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from typing_extensions import Self

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.data_import_tables import DataImportTables


T = TypeVar("T", bound="DataImport")


@_attrs_define
class DataImport:
    """
    Attributes:
        rows (int): How many rows were written.
        cursor (int | Unset): The cursor the export named, to ask the other deployment for changes from.
        tables (DataImportTables | Unset): How many of them went into each table.
        skipped (list[str] | Unset): The rows left out rather than written, by table and primary key, as
            connector_definitions:{"id":"custom_crm","revision":1}. Such a row is a custom connector definition with
            oauth2_client_credentials, which no connection can use: the scheme needs a token endpoint a custom connector
            cannot set.
        skipped_more (int | Unset): How many more rows were left out than skipped names, which holds the first 100.
    """

    rows: int
    cursor: int | Unset = UNSET
    tables: DataImportTables | Unset = UNSET
    skipped: list[str] | Unset = UNSET
    skipped_more: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        rows = self.rows

        cursor = self.cursor

        tables: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tables, Unset):
            tables = self.tables.to_dict()

        skipped: list[str] | Unset = UNSET
        if not isinstance(self.skipped, Unset):
            skipped = self.skipped

        skipped_more = self.skipped_more

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "rows": rows,
            }
        )
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if tables is not UNSET:
            field_dict["tables"] = tables
        if skipped is not UNSET:
            field_dict["skipped"] = skipped
        if skipped_more is not UNSET:
            field_dict["skipped_more"] = skipped_more

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.data_import_tables import DataImportTables

        d = dict(src_dict)
        rows = d.pop("rows")

        cursor = d.pop("cursor", UNSET)

        _tables = d.pop("tables", UNSET)
        tables: DataImportTables | Unset
        if isinstance(_tables, Unset):
            tables = UNSET
        else:
            tables = DataImportTables.from_dict(_tables)

        skipped = cast(list[str], d.pop("skipped", UNSET))

        skipped_more = d.pop("skipped_more", UNSET)

        data_import = cls(
            rows=rows,
            cursor=cursor,
            tables=tables,
            skipped=skipped,
            skipped_more=skipped_more,
        )

        data_import.additional_properties = d
        return data_import

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
