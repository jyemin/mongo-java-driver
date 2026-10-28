# Wire message assertions (POC schema)

Throwaway draft. Modeled on `observeLogMessages` / `expectLogMessages`.

## Client entity: `observeWireMessages`

Optional document. Enables recording of messages the client writes to the server.

    observeWireMessages:
      connections: [application]   # application | monitor | rtt.   Required.
      phases: [handshake]          # handshake | operation.    Required.

- `handshake` = every command written on a connection from connect until the connection is ready: the `hello`/`isMaster`,
  plus any authentication exchange (`saslStart`, `saslContinue`, `authenticate`).
- `operation` = everything after that. For `monitor` and `rtt` this is the streaming or polling `hello`.
- `monitor` = the connection a server monitor uses for its handshake and its (streaming or polling) `hello`s.
  `rtt` = the separate connection used to measure round trip time while the monitor streams.

## Client entity: `setEnvironment`

Optional document of `name: string | null` pairs; `null` unsets the variable. The runner makes these environment variables visible to the driver while the
client entity is being created, then restores the previous environment. Needed because the handshake `client.env` document
is derived from the environment (FaaS and container detection), and it is computed when the client is constructed.
How to do it is left to each runner (the Java runner replaces its internal environment lookup).

## Test field: `expectWireMessages`

Sibling of `expectEvents` / `expectLogMessages`.

    expectWireMessages:
      - client: <client entity id>
        source: application | monitor | rtt   # optional: only compare messages from this source
        ignoreExtraMessages: <bool, default false>
        messages: [ <message>, ... ]

### What a message is

A message is the decoding of the bytes that were written to the socket, and of nothing else. The runner takes the bytes at
the point where the driver hands them to the stream, so a bug anywhere above that point (building the command, encoding the
sections, compressing, framing) is visible. The driver's own message objects are never consulted.

    # OP_MSG
    - opcode: OP_MSG
      responseTo: 0
      flags: []                      # names of the set bits: checksumPresent, moreToCome, exhaustAllowed; others as unknownBit<N>
      sections:
        - kind: 0
          body: { ... }              # the kind-0 document
        - kind: 1
          identifier: documents
          documents: [ { ... }, ... ]

    # OP_QUERY
    - opcode: OP_QUERY
      responseTo: 0
      flags: []                      # tailableCursor, secondaryOk, noCursorTimeout, awaitData, exhaust, partial
      fullCollectionName: admin.$cmd
      numberToSkip: 0
      numberToReturn: -1
      query: { ... }
      # returnFieldsSelector: { ... }   present only if the message carries one

    # OP_COMPRESSED
    - opcode: OP_COMPRESSED
      responseTo: 0
      originalOpcode: OP_MSG | OP_QUERY
      uncompressedSize: <int>
      compressorId: <int>            # 1 snappy, 2 zlib, 3 zstd
      message: { ... }               # the decompressed message, in the form above without opcode and responseTo

- `requestId` is not part of the decoded form. It is an arbitrary value, and no rule constrains it.
- `responseTo` is `0` on every request a driver writes.
- `command` is shorthand. In an OP_MSG it stands for `sections: [{kind: 0, body: <command>}]`, so it also asserts that there is
  exactly one section. In an OP_QUERY it stands for `query: <command>`.
- Matching is strict at every level. A message must list every field it has, and a document must list every key it has, as in
  the unified format's rule for non-root documents. `$$matchAsRoot` allows extra keys. This is what catches a stray
  `client.container`, a misplaced `backpressure`, an extra section, or an unexpected flag.
- Messages are matched in the order they were written. With `source`, only that source's messages are considered, so
  the interleaving of the monitor and RTT threads does not matter. Tests use `directConnection: true`, so each source
  has exactly one connection.
- Nothing is redacted. `saslStart` / `speculativeAuthenticate` payloads are visible, so these tests are only meaningful
  against test credentials.

### Structural checks on every recorded message

The runner decodes every message it recorded, not just the ones a test lists, and fails the test if any of these is violated:

- the header length equals the number of bytes written;
- the opcode is one the decoder knows;
- each section, and each document in it, ends exactly where its length says it does;
- no bytes are left over after the last section;
- if `checksumPresent` is set, the CRC-32C is correct;
- an OP_COMPRESSED message's `uncompressedSize` equals the size of what the decompressor produced.

A runner that has no decompressor for the algorithm in use fails the test and says so.

## Known gaps

- `client.env.container.runtime` comes from the existence of `/.dockerenv`, which a test cannot control. Tests treat
  `runtime` as optional (`$$unsetOrMatches: docker`), so they are not fully strict on Docker hosts. Fixing this needs a
  seam for the file probe plus a runner option to force it on or off (for example `dockerenv: true|false`).

## Operation: `waitForWireMessages`

    - name: waitForWireMessages
      object: testRunner
      arguments: { client: <id>, source: application | monitor | rtt, count: <int> }

Blocks until at least `count` messages from `source` have been recorded for the client, then returns. Fails after a
timeout (10 s in the Java runner), reporting what was recorded. Needed because monitors run on their own threads.

Monitor and RTT connections keep sending for as long as the client lives, so tests on them MUST set
`ignoreExtraMessages: true` and assert only the first N messages. The first N messages of one connection are deterministic;
the total at the time of the assertion is not. Tests do not close the client first: closing does not wait for the monitor
threads, so it does not make the recorded list final.

## Client entity uriOptions used by these tests

`directConnection`, `serverMonitoringMode`, `heartbeatFrequencyMS`, `compressors`, `authMechanism: SCRAM-SHA-256`.
