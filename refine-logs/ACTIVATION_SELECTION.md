# CacheKit P0 activation selection

The immutable early-lookahead canary set is `q9`.

| Query | Valid control | Worker tasks built/executed | Prepared | Staged | Promoted | Live-read races | Decision |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| q9 | yes | 247,355 / 247,355 | 1,570,117 | 0 | 0 | 1,081,170 | select |
| q16 | yes | 0 / 0 | 23,314,404 | 23,314,404 | 22,968,831 | 0 | exclude from early-lookahead canary |
| q17 | yes | 0 / 0 | 1,574,908 | 1,574,908 | 1,569,492 | 0 | exclude from early-lookahead canary |

q16 and q17 exercise ValueState, but their contemporary controls use synchronous
preparation and already promote almost all prepared values. q9 is the measured
worker/mailbox race: it built 247,355 tasks, recorded 1,081,170 live-read races,
cancelled 1,078,984 live reads, dropped 80,614 shrunken batches, and promoted no
staged values. P0 therefore tunes q9 first; any winner must still pass the full
15-query gate.
