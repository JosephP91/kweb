from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from ..command import ICommand
	from ..context import ConsumerContext

from .exception import UnsupportedCommandException
from .type import CommandName


class CommandFactory:
	@staticmethod
	def get_instance(cmd_name: str, context: ConsumerContext) -> ICommand:
		try:
			return CommandName[cmd_name.upper()].value(context)
		except KeyError:
			raise UnsupportedCommandException(cmd_name)

