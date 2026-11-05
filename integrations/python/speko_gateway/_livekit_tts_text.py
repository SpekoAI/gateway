"""Lossless, bounded native speech commits without tokenizer marker strings."""

from __future__ import annotations

import re

_MAX_CHARACTERS = 240
_SENTENCE_END = re.compile(
    r"""\.["'”’»」』）)\]]*(?=\s)|[!?]["'”’»」』）)\]]*(?=\s|$)|[。！？؟]["'”’»」』）)\]]*|\r\n|[\r\n]"""
)
_TRAILING_PERIOD = re.compile(r"""(\S+)\.["'”’»」』）)\]]*$""")


class NativeTextBuffer:
    def __init__(self) -> None:
        self._text = ""

    def push(self, token: str) -> list[str]:
        self._text += token
        ready = []
        while self._text:
            boundary = _SENTENCE_END.search(self._text)
            if boundary is not None and boundary.end() <= _MAX_CHARACTERS:
                end = boundary.end()
            elif len(self._text) >= _MAX_CHARACTERS:
                # Prefer a word boundary, retaining its whitespace in the
                # next commit. Unbroken text still has a finite first commit.
                end = self._text.rfind(" ", 0, _MAX_CHARACTERS + 1)
                if end <= 0:
                    end = _MAX_CHARACTERS
            else:
                break
            ready.append(self._text[:end])
            self._text = self._text[end:]
        return ready

    def flush(self) -> str:
        text, self._text = self._text, ""
        return text

    @property
    def can_flush_idle(self) -> bool:
        # A token boundary alone cannot complete a period: the next token
        # may extend a decimal or hostname. All terminal periods get the
        # feeder's bounded lookahead, including numeric/dotted endings: a
        # completed "Call 911." must not wait indefinitely for another token.
        return _TRAILING_PERIOD.search(self._text) is not None
