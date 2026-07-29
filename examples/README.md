# Examples

All examples assume v2 is installed from the repository checkout:

```bash
python -m pip install .
```

The core package remains offline and dependency-free. Examples that add a web
framework or contact a customer system say so explicitly.

## Single address

```bash
python examples/basic.py
python examples/basic.py "г. Москва, ул. Тверская, д.4, кв.12"
```

The program prints the full JSON-compatible result followed by its review
decision.

## Address span in a message

```bash
python examples/detect_in_message.py
```

The detector returns ordered half-open spans into the original message and a
`ParsedAddress` for each span. It deliberately requires strong address evidence
and does not treat every place name or number as an address.

## Streaming ETL

Each input line is treated as one raw address. Each output line is one JSON
object, so memory use does not grow with the file:

```bash
printf '%s\n' \
  'Ополченская 5-30' \
  'Самара Авроры 7 12' |
  python examples/jsonl_etl.py > parsed.jsonl
```

Blank lines are retained as empty parse results. Add a filter in the calling
pipeline if blank lines should instead be rejected.

## FastAPI wrapper

FastAPI and Uvicorn are optional application dependencies, not package runtime
dependencies:

```bash
python -m pip install fastapi uvicorn
uvicorn examples.fastapi_app:app --reload
```

Then:

```bash
curl -sS http://127.0.0.1:8000/parse \
  -H 'content-type: application/json' \
  -d '{"address":"СПб Невский проспект 10 корп 2 кв 15"}'
```

Add the authentication, request-size limits, rate limits, observability, and
deployment controls required by your environment before exposing this service.

## Customer-managed FIAS/GAR resolver

First inspect the proposed generic request without making a network call:

```bash
python examples/fias_gar_http.py "Ополченская 5-30"
```

To send it to an endpoint you control:

```bash
python examples/fias_gar_http.py \
  "Ополченская 5-30" \
  --resolver-url https://resolver.internal.example/v1/candidates \
  --send
```

The example defines an illustrative HTTP contract; FIAS/GAR does not impose
that contract. Adapt field names, authentication, candidate ranking, and
registry freshness policy to your system. `--send` is explicit because the
parser itself must never hide a network call.
