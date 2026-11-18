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
        # Do not guess whether an abbreviation ends a sentence or introduces
        # the already-buffered words. Keep all available context together;
        # push() has already bounded this remainder to <240 characters.
        for ending in reversed(list(_PERIOD_END.finditer(self._text))):
            word = _word_before_period(self._text, ending.start()).lower()
            end = ending.end()
            continuation = bool(self._text[end:].strip())
            if continuation and _abbreviation(self._text, ending.start()):
                return len(self._text)
            # Standalone St. may be a complete street suffix. An isolated
            # title/unit prefix still waits for its following word.
            if word in _CONTINUING_PREFIXES and word != "st":
                continue
            return end if continuation else len(self._text)
        return None

    def flush_idle(self) -> str:
        end = self._idle_end()
        if end is None:
            return ""
        text, self._text = self._text[:end], self._text[end:]
        return text

    @property
    def can_flush_idle(self) -> bool:
        # Completed prefixes remain eligible; available abbreviation context
        # travels with its prefix, while other unfinished suffixes stay local.
        return self._idle_end() is not None
