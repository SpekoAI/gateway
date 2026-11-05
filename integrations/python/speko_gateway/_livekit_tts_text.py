"""Lossless, bounded native speech commits without tokenizer marker strings."""

from __future__ import annotations

import re

_MAX_CHARACTERS = 240
_SENTENCE_END = re.compile(
    r"""\.["'”’»」』）)\]]*(?=\s)|[!?]["'”’»」』）)\]]*(?=\s|$)|[。！？؟]["'”’»」』）)\]]*|\r\n|[\r\n]"""
)
_TRAILING_PERIOD = re.compile(r"""\.["'”’»」』）)\]]*\s*$""")
_PERIOD_WORD = re.compile(r"[A-Za-z]+(?:\.[A-Za-z]+)*$")
_CONTINUING_PREFIXES = frozenset({
    "mr", "mrs", "ms", "dr", "prof", "st", "capt", "cpt", "lt", "col",
    "gen", "rev", "sgt", "fl",
})
_ABBREVIATIONS = _CONTINUING_PREFIXES | frozenset({
    "jr", "sr", "inc", "ltd", "co", "vs", "etc", "fig", "no", "vol",
    "oz", "lb", "lbs", "tsp", "tbsp", "ft", "in", "a.m", "p.m",
    "i.e", "e.g", "ph.d",
})


def _word_before_period(text: str, position: int) -> str:
    match = _PERIOD_WORD.search(text, 0, position)
    return match.group() if match is not None else ""


def _abbreviation(text: str, position: int) -> bool:
    word = _word_before_period(text, position)
    return (
        word.lower() in _ABBREVIATIONS
        or len(word) == 1 and word.isupper()
        or re.fullmatch(r"(?:[A-Za-z]\.)+[A-Za-z]", word) is not None
    )


class NativeTextBuffer:
    def __init__(self) -> None:
        self._text = ""

    def push(self, token: str) -> list[str]:
        self._text += token
        ready = []
        while self._text:
            boundary = next(
                (match for match in _SENTENCE_END.finditer(self._text)
                 if self._text[match.start()] != "."
                 or not _abbreviation(self._text, match.start())),
                None,
            )
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
        ending = _TRAILING_PERIOD.search(self._text)
        return ending is not None and (
            _word_before_period(self._text, ending.start()).lower()
            not in _CONTINUING_PREFIXES
        )
