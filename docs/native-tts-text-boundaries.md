# Native TTS text boundaries

The selected streaming contract holds unfinished words until a real delimiter
or explicit completion. This is the user-approved replacement for idle flushing
of undelimited text. The adapter does not treat a pause, a local timer or an LLM
chunk boundary as proof that a word is complete.

`TTS.stream().push_text()` accepts arbitrary pieces. `Ple` can become `Please`;
`St. Louis` can become `St. Louisville`; `of water` can become `of waterfall`.
The same ambiguity applies to decimal and domain periods. A bare `0.` or
`example.` therefore remains buffered even beyond the former 300 ms idle pause.
Whitespace or an unambiguous sentence delimiter provides an observed boundary.
An explicit stream `flush()` / native `commit_text()`, or EOF, finalizes the
producer's remaining text. Existing controls and wire messages are unchanged.

The 300 ms idle lookahead still applies to eligible abbreviation context whose
words have already been confirmed. It is never increased. Producers that wait
for audio must first provide a real text boundary or explicitly complete their
utterance; an undelimited paused prefix has no liveness guarantee. This includes
previous examples using `Main St.` or `Call 911.` without a following delimiter.
Their old idle behavior is intentionally withdrawn under the selected contract.

All characters and separators are retained. No tokenizer marker, guessed word,
substituted text or synthesized completion event is introduced. The existing
240-character utterance bound counts Python `str` Unicode code points, including
combining marks separately. It is not a UTF-8 byte count or a grapheme count.
Bounded commits cut at observed whitespace. An undelimited unit of exactly 240
code points may wait for a delimiter, flush or EOF; a longer unit is refused
without retry rather than split across provider utterances. Invalid buffered
text is cleared before failure cleanup can flush it.

LiveKit's default Markdown transform can keep a trailing space or punctuation
while it resolves its own input. The adapter waits for the transform's real
output or EOF; default AgentSession completion remains supported. A producer
with already plain text can use the existing public
`AgentSession(..., tts_text_transforms=[])` option to expose its original
boundaries immediately. Neither setting supplies admission or financial proof.

The gated plain-text AgentSession tests use that option. Gated direct-stream
fixtures move the existing leading separator from their later input to their
first input, leaving the complete input bytes and exact provider text/PCM
assertions unchanged. Separate default-transform AgentSession controls retain
its actual filtering, then verify whole words, canonical text, PCM and one
physical request at real completion. The original midword regressions and
assertions remain unchanged, including a pause beyond 300 ms. Actual late
numeric/domain controls retain the original full-value assertions.

A local text refusal may occur after a native session was configured. It proves
neither remote non-execution nor NoIO, receipt delivery, settlement or capacity
release. Native automatic retries remain disabled; source/factory/paid-provider
acceptance and the production supported matrix remain separately gated.
