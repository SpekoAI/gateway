"""Lossless, bounded native speech commits without tokenizer marker strings."""

from __future__ import annotations

import re

from .client import GatewayError

_MAX_CHARACTERS = 240
_SENTENCE_END = re.compile(
    r"""\.["'”’»」』）)\]]*(?=\s)|[!?]["'”’»」』）)\]]*(?=\s|$)|[。！？؟]["'”’»」』）)\]]*|\r\n|[\r\n]"""
)
_PERIOD_END = re.compile(r"""\.["'”’»」』）)\]]*(?=\s|$)""")
_PERIOD_WORD = re.compile(r"[A-Za-z]+(?:\.[A-Za-z]+)*$")
_CONTINUING_PREFIXES = frozenset(
    {
        "mr",
        "mrs",
        "ms",
        "dr",
        "prof",
        "st",
        "capt",
        "cpt",
        "lt",
        "col",
        "gen",
        "rev",
        "sgt",
        "fl",
    }
)
_ABBREVIATIONS = _CONTINUING_PREFIXES | frozenset(
    {
        "jr",
        "sr",
        "inc",
        "ltd",
        "co",
        "vs",
        "etc",
        "fig",
        "no",
        "vol",
        "oz",
        "lb",
        "lbs",
        "tsp",
        "tbsp",
        "ft",
        "in",
        "a.m",
        "p.m",
        "i.e",
        "e.g",
        "ph.d",
    }
)


def _word_before_period(text: str, position: int) -> str:
    match = _PERIOD_WORD.search(text, 0, position)
    return match.group() if match is not None else ""


def _abbreviation(text: str, position: int) -> bool:
    word = _word_before_period(text, position)
    return (
        word.lower() in _ABBREVIATIONS
        or len(word) == 1
        and word.isupper()
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
                (
                    match
                    for match in _SENTENCE_END.finditer(self._text)
                    if self._text[match.start()] != "."
                    or not _abbreviation(self._text, match.start())
                ),
                None,
            )
            if boundary is not None and boundary.end() <= _MAX_CHARACTERS:
                end = boundary.end()
            elif len(self._text) >= _MAX_CHARACTERS:
                # A bound cannot turn an unfinished word into a completed one.
                # Retain separators verbatim and cut only at an observed one.
                boundaries = [
                    match.start()
                    for match in re.finditer(r"\s", self._text[: _MAX_CHARACTERS + 1])
                ]
                if not boundaries:
                    if len(self._text) > _MAX_CHARACTERS:
                        # feed() flushes its remainder during cleanup. Invalid
                        # oversized input must not escape through that flush.
                        self._text = ""
                        raise GatewayError(
                            "Native TTS word exceeded its bounded utterance",
                            retryable=False,
                        )
                    break
                end = max(1, boundaries[-1])
            else:
                break
            ready.append(self._text[:end])
            self._text = self._text[end:]
        return ready

    def flush(self) -> str:
        text, self._text = self._text, ""
        return text

    def _idle_end(self) -> int | None:
        # No elapsed pause proves that an unterminated word, number or domain
        # is complete. A producer delimiter (or explicit flush/EOF) must do so.
        if not self._text or not self._text[-1].isspace():
            return None
        confirmed_end = len(self._text.rstrip())
        # Keep confirmed abbreviation context together and retain separators
        # locally for the next commit. No characters are normalized or lost.
        for ending in reversed(list(_PERIOD_END.finditer(self._text))):
            word = _word_before_period(self._text, ending.start()).lower()
            end = ending.end()
            continuation = bool(self._text[end:confirmed_end].strip())
            if continuation and _abbreviation(self._text, ending.start()):
                return confirmed_end
            # Standalone St. may be a complete street suffix. An isolated
            # title/unit prefix still waits for its following word.
            if word in _CONTINUING_PREFIXES and word != "st":
                continue
            return end if continuation else confirmed_end
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
