from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..context import Context


class Response:
    @staticmethod
    def success(cmd_name: str, ctx: Context, **payload) -> dict:
        ctx.logger.info("[{}] - Executed '{}'".format(ctx.client_id, cmd_name))
        return {
            "command_name": cmd_name,
            "client_id": ctx.client_id,
            "payload": payload
        }

    @staticmethod
    def cmd_error(cmd_name: str, ctx: Context, reason: Exception):
        ctx.logger.error("[{}] - Error '{}': {}".format(ctx.client_id, cmd_name, str(reason)))
        return {
            "command_name": cmd_name, 
            "client_id": ctx.client_id, 
            "reason": str(reason)
        }

    @staticmethod
    def error(ctx: Context, reason: Exception):
        ctx.logger.error("[{}] - Error: {}".format(ctx.client_id, str(reason)))
        return {
            "client_id": ctx.client_id,
            "reason": str(reason)
        }

