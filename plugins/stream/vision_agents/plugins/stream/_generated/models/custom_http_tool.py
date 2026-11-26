from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from typing_extensions import Self

from ..models.custom_http_tool_method import CustomHTTPToolMethod
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.custom_http_tool_parameters import CustomHTTPToolParameters


T = TypeVar("T", bound="CustomHTTPTool")


@_attrs_define
class CustomHTTPTool:
    """One tool of an http connector: one request, whose method and path are fixed.

    Attributes:
        description (str): What the tool does, which the model reads to know when to use it. Up to 1000 bytes.
        method (CustomHTTPToolMethod):
        name (str): 1 to 63 letters, digits, _ or -. The model is offered it after the binding's name and __.
        path (str): Under base_url, starting with /, without a query or fragment. Each {name} in it is the parameter of
            that name, which must be required.
        parameters (CustomHTTPToolParameters | Unset): The values the model gives, by name, up to 64. A parameter not in
            the path goes into the query of a GET, HEAD or DELETE and into a JSON body otherwise. A name a client reads as a
            method override, such as _method or X-HTTP-Method-Override, is refused.
        read_only (bool | Unset): The tool only reads. Only a GET or HEAD tool may say so.
    """

    description: str
    method: CustomHTTPToolMethod
    name: str
    path: str
    parameters: CustomHTTPToolParameters | Unset = UNSET
    read_only: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        method = self.method.value

        name = self.name

        path = self.path

        parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters.to_dict()

        read_only = self.read_only

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "description": description,
                "method": method,
                "name": name,
                "path": path,
            }
        )
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if read_only is not UNSET:
            field_dict["read_only"] = read_only

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.custom_http_tool_parameters import CustomHTTPToolParameters

        d = dict(src_dict)
        description = d.pop("description")

        method = CustomHTTPToolMethod._decode(d.pop("method"))

        name = d.pop("name")

        path = d.pop("path")

        _parameters = d.pop("parameters", UNSET)
        parameters: CustomHTTPToolParameters | Unset
        if isinstance(_parameters, Unset):
            parameters = UNSET
        else:
            parameters = CustomHTTPToolParameters.from_dict(_parameters)

        read_only = d.pop("read_only", UNSET)

        custom_http_tool = cls(
            description=description,
            method=method,
            name=name,
            path=path,
            parameters=parameters,
            read_only=read_only,
        )

        return custom_http_tool
