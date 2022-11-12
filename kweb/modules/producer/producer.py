from ..actor import AsyncActor
from ..command import CommandQueueEmptyException


class AsyncProducer(AsyncActor):
    def _callback(self):
        self.logger.info("[{}] - Starting producer.".format(self.ctx.client_id))
        while not self._should_stop:
            try:
                queued_cmd = self.ctx.cmd_queue.pop(block=True, timeout=1)
                cmd_output = queued_cmd.command.execute(queued_cmd.parameters)
                self._spawn_ioloop_callback(self.ctx.on_actor_data, cmd_output)
            except CommandQueueEmptyException:
                pass

        self.logger.info("[{}] - Stopping producer.".format(self.ctx.client_id))
        if self.ctx.producer is not None:
            self.logger.info("[{}] - Closing producer.".format(self.ctx.client_id))
            self.ctx.producer.close()
