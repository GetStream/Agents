from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from typing_extensions import Self

from ..types import UNSET, Unset

T = TypeVar("T", bound="AgentIdentity")


@_attrs_define
class AgentIdentity:
    """Who an agent config's agent is in Stream Chat, set by the app's backend. An unknown field is refused rather than
    ignored.

        Attributes:
            agent_id (str | Unset): The agent id a new conversation is kept under, as a session's agent_id. A text
                conversation's replies are written as the Chat user of this id, which the router makes with user_name when the
                app has none. It may not start with !.
            user_id (str | Unset): The Stream user the agent joins a call and writes Chat messages as, as a session's
                user_id. Up to 128 letters, digits, _, . and -, not starting with !.
            user_name (str | Unset): That user's display name, as a session's user_name, and the name the router gives the
                agent_id's Chat user when it makes it.
    """

    agent_id: str | Unset = UNSET
    user_id: str | Unset = UNSET
    user_name: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        agent_id = self.agent_id

        user_id = self.user_id

        user_name = self.user_name

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if agent_id is not UNSET:
            field_dict["agent_id"] = agent_id
        if user_id is not UNSET:
            field_dict["user_id"] = user_id
        if user_name is not UNSET:
            field_dict["user_name"] = user_name

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        agent_id = d.pop("agent_id", UNSET)

        user_id = d.pop("user_id", UNSET)

        user_name = d.pop("user_name", UNSET)

        agent_identity = cls(
            agent_id=agent_id,
            user_id=user_id,
            user_name=user_name,
        )

        return agent_identity
