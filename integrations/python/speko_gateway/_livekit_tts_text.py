"""Lossless, bounded native speech commits without tokenizer marker strings."""

from __future__ import annotations

import re

_MAX_CHARACTERS = 240
_SENTENCE_END = re.compile(
    r"""\.["'”’»」』）)\]]*(?=\s)|[!?]["'”’»」』）)\]]*(?=\s|$)|[。！？؟]["'”’»」』）)\]]*|\r\n|[\r\n]"""
)
_PERIOD_END = re.compile(r"""\.["'”’»」』）)\]]*(?=\s|$)""")
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

    def _idle_end(self) -> int | None:
        # St. is also a complete street suffix. Keep it with a continuation
        # during the same bounded lookahead, without treating it as an
        # indefinitely incomplete title. Dr./fl. retain their required noun.
        for ending in reversed(list(_PERIOD_END.finditer(self._text))):
            word = _word_before_period(self._text, ending.start()).lower()
            if word in _CONTINUING_PREFIXES and word != "st":
                continue
            end = ending.end()
            return len(self._text) if not self._text[end:].strip() else end
        return None

    def flush_idle(self) -> str:
        end = self._idle_end()
        if end is None:
            return ""
        text, self._text = self._text[:end], self._text[end:]
        return text

    @property
    def can_flush_idle(self) -> bool:
        # A complete prefix remains eligible even with an unfinished next
        # clause in the buffer. Keep that suffix for the producer's next token.
        return self._idle_end() is not None
