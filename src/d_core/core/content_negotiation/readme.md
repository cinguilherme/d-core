# Content Negotiation

Centralized content negotiation for the messaging platform.

## Goal

Provide a consistent abstraction for determining and transforming data formats across services. When a message arrives with an `Accept` header or a specific content type, this package resolves the preferred format and handles the conversion — whether that's EDN to JSON, EDN to plaintext, JSON to EDN, or any other combination.

## What it does

- **Negotiates format** from HTTP `Accept` headers (including quality factors)
- **Maps MIME types** to internal format identifiers (`:edn`, `:json`, `:plaintext`, `:bytes`)
- **Encodes and decodes** values across formats using the existing codecs pipeline
- **Provides a single entry point** for format resolution so no service needs to reimplement content negotiation logic

## Supported formats

| Format  | MIME type(s)                    | Codec        |
|---------|----------------------------------|--------------|
| EDN     | `application/edn`              | `d-core.core.codecs.edn`  |
| JSON    | `application/json`             | `d-core.core.codecs.json` |
| Bytes   | `application/octet-stream`     | `d-core.core.codecs.bytes`|
| ByteBuffer | `application/octet-stream`  | `d-core.core.codecs.byte-buffer` |

## Relationship to codecs

Content negotiation sits above the codecs layer. It selects the appropriate format and delegates encoding/decoding to the corresponding codec. The codecs package handles the low-level serialization; content negotiation handles the high-level format resolution.
