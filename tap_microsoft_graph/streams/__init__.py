"""Stream definitions for tap-microsoft-graph."""

from tap_microsoft_graph.streams.attachments import AttachmentsStream
from tap_microsoft_graph.streams.messages import MessagesStream

__all__ = ["AttachmentsStream", "MessagesStream"]
