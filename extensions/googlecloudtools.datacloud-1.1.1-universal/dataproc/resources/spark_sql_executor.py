"""Spark SQL query execution daemon using Dataproc Spark Connect.

Each daemon process backs exactly one SQL editor and owns exactly one Spark
session, so two editors never share session state. The extension host is
responsible for starting one process per editor and restarting it when that
editor's project, region, or session template changes.

Protocol over stdin/stdout:
- The daemon reads JSON-serialized requests from stdin, one per line.
- Each request must be a JSON object with 'id', 'project', and 'region'
  fields, and optionally 'query' and 'sessionTemplate' fields.
- The daemon responds with JSON-serialized payloads on stdout, followed by a
  newline.
- Initial response upon startup (after Spark Connect initialization):
  {"status": "ready"}
- If initialization fails:
  {"error": "Failed to initialize Spark session: ...", "trace": "..."}
- If a request is missing 'project' or 'region':
  {"id": req_id, "error": "Project and region are required in payload"}
- For each valid request with a valid query:
  {"id": req_id, "results": [...]}
- If a request is invalid or missing a query:
  {"id": req_id, "error": "No query provided"}
- If execution of a statement fails:
  {
      "id": req_id,
      "results": [
          {"query": "stmt1", "queryResults": ...},
          {"query": "stmt2", "error": "..."}
      ]
  }
"""

import atexit
import base64
import datetime
import json
import re
import signal
import sys
import threading
import time
import traceback
from typing import Any

# pylint: disable=g-import-not-at-top
try:
  from google.cloud.managed_spark_connect import ManagedSparkSession  # pyrefly: ignore[missing-import]
except ImportError:
  ManagedSparkSession = None

try:
  from google.cloud import dataproc_v1  # pyrefly: ignore[missing-import]
except ImportError:
  dataproc_v1 = None


# Each daemon process serves exactly one SQL editor, so it owns exactly one
# Spark session. Isolation between editors comes from running separate
# processes rather than from keying a cache.
_SESSION = None
# The (project, region, session_template) tuple that _SESSION was created with.
_SESSION_CONFIG = None
_SESSION_LOCK = threading.Lock()
_SEND_LOCK = threading.Lock()


# TODO(b/562500821) - Move this SQL splitting logic upstream to the typeScript
#  extension host.
def split_statements(sql: str) -> list[str]:
  """Splits a SQL script into individual statements by semicolon."""
  statements = []
  current = []
  in_single_quote = False
  in_double_quote = False
  in_backtick = False
  in_line_comment = False
  in_block_comment = False
  i = 0
  n = len(sql)

  while i < n:
    ch = sql[i]
    next_ch = sql[i + 1] if i + 1 < n else ''

    if in_line_comment:
      current.append(ch)
      if ch == '\n':
        in_line_comment = False
      i += 1
      continue

    if in_block_comment:
      current.append(ch)
      if ch == '*' and next_ch == '/':
        current.append(next_ch)
        in_block_comment = False
        i += 2
        continue
      i += 1
      continue

    if in_single_quote:
      current.append(ch)
      if ch == '\\':
        if next_ch:
          current.append(next_ch)
          i += 2
          continue
      elif ch == "'":
        if next_ch == "'":
          current.append(next_ch)
          i += 2
          continue
        in_single_quote = False
      i += 1
      continue

    if in_double_quote:
      current.append(ch)
      if ch == '\\':
        if next_ch:
          current.append(next_ch)
          i += 2
          continue
      elif ch == '"':
        if next_ch == '"':
          current.append(next_ch)
          i += 2
          continue
        in_double_quote = False
      i += 1
      continue

    if in_backtick:
      current.append(ch)
      if ch == '`':
        in_backtick = False
      i += 1
      continue

    if ch == '-' and next_ch == '-':
      current.append(ch)
      current.append(next_ch)
      in_line_comment = True
      i += 2
      continue

    if ch == '/' and next_ch == '*':
      current.append(ch)
      current.append(next_ch)
      in_block_comment = True
      i += 2
      continue

    if ch == "'":
      in_single_quote = True
      current.append(ch)
      i += 1
      continue

    if ch == '"':
      in_double_quote = True
      current.append(ch)
      i += 1
      continue

    if ch == '`':
      in_backtick = True
      current.append(ch)
      i += 1
      continue

    if ch == ';':
      stmt = ''.join(current).strip()
      if stmt:
        statements.append(stmt)
      current = []
      i += 1
      continue

    current.append(ch)
    i += 1

  stmt = ''.join(current).strip()
  if stmt:
    statements.append(stmt)

  return statements


def send_json(payload: dict[str, Any]) -> None:
  """Sends a JSON-serialized payload to stdout."""
  with _SEND_LOCK:
    print(json.dumps(payload), flush=True)


def serialize_dataframe(
    df: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
  """Serializes a Spark DataFrame to rows and schema fields."""
  schema_fields = []
  if df is not None and df.schema is not None:
    for field in df.schema.fields:
      schema_fields.append(
          {'name': field.name, 'type': field.dataType.typeName()}
      )

  collected = df.collect() if df is not None else []
  rows = []
  for row in collected:
    cells = []
    for field in df.schema.fields:
      val = row[field.name]
      # Handle basic conversions
      if isinstance(val, (datetime.date, datetime.datetime)):
        val = val.isoformat()
      elif isinstance(val, bytes):
        val = base64.b64encode(val).decode('utf-8')
      elif isinstance(val, (int, float, bool, str)) or val is None:
        pass
      else:
        val = str(val)  # Fallback
      cells.append({'v': val})
    rows.append({'f': cells})

  return rows, schema_fields


def execute_statements(
    spark: Any, statements: list[str]
) -> list[dict[str, Any]]:
  """Executes a list of SQL statements and returns the results."""
  results = []
  for stmt in statements:
    try:
      df = spark.sql(stmt)
      rows, schema_fields = serialize_dataframe(df)
      results.append({
          'query': stmt,
          'queryResults': {
              'rows': rows,
              'schema': {'fields': schema_fields},
              'totalRows': len(rows),
          },
      })
    except Exception as stmt_err:  # pylint: disable=broad-exception-caught
      results.append({
          'query': stmt,
          'error': str(stmt_err),
      })
      break
  return results


def is_session_inactive_error(err_str: str) -> bool:
  """Checks if the error string indicates an inactive Spark session."""
  err_str = err_str.lower()
  return (
      'session not active' in err_str
      or 'session is closed' in err_str
      or 'session does not exist' in err_str
      or 'session not found' in err_str
      or ('session' in err_str and 'not active' in err_str)
  )


def reset_session() -> None:
  """Drops the cached session so the next request creates a fresh one."""
  global _SESSION, _SESSION_CONFIG
  with _SESSION_LOCK:
    stale = _SESSION
    _SESSION = None
    _SESSION_CONFIG = None

  if stale is not None:
    try:
      stale.stop()
    except Exception as e:  # pylint: disable=broad-exception-caught
      # The session is already being discarded, so a failure to stop it
      # cleanly must not prevent a new one from being created.
      print(f'Failed to stop stale Spark session: {e}', file=sys.stderr)


def is_session_cached(
    project: str, region: str, session_template: str | None
) -> bool:
  """Returns whether a Spark session is already cached for this daemon."""
  with _SESSION_LOCK:
    return _SESSION is not None and _SESSION_CONFIG == (
        project,
        region,
        session_template,
    )


_LAKEHOUSE_PUBLIC_DATA_CATALOG = 'lakehouse-public-data'
_BIGLAKE_PUBLIC_DATA_PROJECT = 'bigquery-public-data'


def _get_lakehouse_spark_configs(project: str) -> dict[str, str]:
  """Returns Spark configs to register Lakehouse catalogs including lakehouse-public-data."""
  catalog_prefix = f'spark.sql.catalog.{_LAKEHOUSE_PUBLIC_DATA_CATALOG}'
  return {
      'dataproc.lakehouse.catalog.lakehouse-public-data': (
          f'projects/{_BIGLAKE_PUBLIC_DATA_PROJECT}/catalogs/'
          f'{_LAKEHOUSE_PUBLIC_DATA_CATALOG}'
      ),
      'spark.dataproc.lakehouse.project': project,
      'spark.sql.extensions': (
          'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
      ),
      catalog_prefix: 'org.apache.iceberg.spark.SparkCatalog',
      f'{catalog_prefix}.type': 'rest',
      f'{catalog_prefix}.uri': (
          'https://biglake.googleapis.com/iceberg/v1/restcatalog'
      ),
      f'{catalog_prefix}.warehouse': f'gs://{_LAKEHOUSE_PUBLIC_DATA_CATALOG}',
      f'{catalog_prefix}.header.x-goog-user-project': project,
      f'{catalog_prefix}.rest.auth.type': (
          'org.apache.iceberg.gcp.auth.GoogleAuthManager'
      ),
      f'{catalog_prefix}.rest-metrics-reporting-enabled': 'false',
      f'{catalog_prefix}.io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
      f'{catalog_prefix}.header.X-Iceberg-Access-Delegation': (
          'vended-credentials'
      ),
  }


def _configure_lakehouse_catalogs(spark: Any, project: str) -> None:
  """Ensures runtime-modifiable Lakehouse catalog properties are set on the session."""
  conf = getattr(spark, 'conf', None)
  if conf is None:
    return
  for key, value in _get_lakehouse_spark_configs(project).items():
    if key.startswith('spark.sql.catalog.'):
      try:
        conf.set(key, value)
      except Exception:  # pylint: disable=broad-exception-caught
        pass


_SESSION_TEMPLATE_RESOURCE_RE = re.compile(
    r'^projects/[^/]+/locations/[^/]+/sessionTemplates/([^/]+)$'
)


def _apply_session_template(
    builder: Any, project: str, region: str, session_template: str
) -> Any:
  """Applies a Dataproc session template to a ManagedSparkSession builder.

  google-cloud-spark-connect 2.0 removed `builder.dataprocSessionConfig` and
  added `builder.sessionTemplate`, which takes the short template ID. Older
  versions only support passing a `dataproc_v1.Session` to
  `dataprocSessionConfig`. The extension installs the package unpinned, so
  support both.

  Args:
    builder: The ManagedSparkSession builder.
    project: The Google Cloud project ID.
    region: The Dataproc region.
    session_template: A short template ID or a full
      `projects/{p}/locations/{l}/sessionTemplates/{id}` resource name.

  Returns:
    The builder with the session template applied.

  Raises:
    RuntimeError: If the installed library supports neither API.
  """
  set_template = getattr(builder, 'sessionTemplate', None)
  if callable(set_template):
    match = _SESSION_TEMPLATE_RESOURCE_RE.match(session_template)
    return set_template(match.group(1) if match else session_template)

  # google-cloud-spark-connect < 2.0.
  set_session_config = getattr(builder, 'dataprocSessionConfig', None)
  if not callable(set_session_config):
    raise RuntimeError(
        'The installed google-cloud-spark-connect version does not support'
        ' session templates (neither builder.sessionTemplate nor'
        ' builder.dataprocSessionConfig exists).'
    )
  if dataproc_v1 is None:
    raise RuntimeError('google-cloud-dataproc is not installed.')
  template_name = session_template
  if not template_name.startswith('projects/'):
    template_name = (
        f'projects/{project}/locations/{region}/'
        f'sessionTemplates/{template_name}'
    )
  session = dataproc_v1.Session()
  session.session_template = template_name
  return set_session_config(session)


def get_spark_session(
    project: str, region: str, session_template: str | None
) -> Any:
  """Returns this process's Spark session, creating it on first use."""
  global _SESSION, _SESSION_CONFIG
  config = (project, region, session_template)

  with _SESSION_LOCK:
    if _SESSION is not None:
      if _SESSION_CONFIG == config:
        return _SESSION
      # The extension host owns one daemon per editor and is expected to
      # restart the daemon when the editor's query settings change, so a
      # mismatch here means that contract was violated. Failing loudly is
      # better than silently answering with a session the caller did not ask
      # for.
      raise RuntimeError(
          'This Spark daemon is already bound to'
          f' {_SESSION_CONFIG} and cannot serve {config}.'
      )

  if ManagedSparkSession is None:
    raise RuntimeError('google-cloud-spark-connect is not installed.')

  builder = (
      ManagedSparkSession.builder.projectId(project)
      .location(region)
      .subnetwork('default-auto')
  )
  for key, value in _get_lakehouse_spark_configs(project).items():
    builder.config(key, value)

  if session_template:
    builder = _apply_session_template(
        builder, project, region, session_template
    )

  # Session creation is slow, so it deliberately happens outside the lock.
  spark = builder.getOrCreate()
  _configure_lakehouse_catalogs(spark, project)

  with _SESSION_LOCK:
    if _SESSION is None:
      _SESSION = spark
      _SESSION_CONFIG = config
    return _SESSION


def extract_session_id(spark: Any) -> str | None:
  """Extracts the Dataproc Serverless session ID if available."""
  session_id = getattr(ManagedSparkSession, '_active_s8s_session_id', None)
  if session_id:
    return str(session_id)
  session_id = getattr(spark, 'session_id', None)
  if session_id:
    return str(session_id)
  client = getattr(spark, '_client', None)
  if client:
    session_id = getattr(client, 'session_id', None)
    if session_id:
      return str(session_id)
  return None


def main():
  if ManagedSparkSession is None:
    send_json({'error': 'google-cloud-spark-connect package is not installed.'})
    sys.exit(1)

  atexit.register(reset_session)

  def handle_signal(signum: int, frame: Any) -> None:
    del signum, frame
    reset_session()
    sys.exit(0)

  try:
    signal.signal(signal.SIGTERM, handle_signal)
  except (ValueError, AttributeError):
    pass

  try:
    signal.signal(signal.SIGINT, handle_signal)
  except (ValueError, AttributeError):
    pass

  send_json({'status': 'ready'})

  def process_line(line: str):
    req_id = None
    try:
      req = json.loads(line)
      req_id = req.get('id')

      if req.get('action') in ('stop', 'close'):
        reset_session()
        send_json({'id': req_id, 'status': 'stopped'})
        sys.exit(0)

      query = req.get('query')
      project = req.get('project')
      region = req.get('region')
      session_template = req.get('sessionTemplate')

      if not project or not region:
        send_json({
            'id': req_id,
            'error': 'Project and region are required in payload',
        })
        return

      session_was_cached = is_session_cached(project, region, session_template)
      startup_started = time.monotonic()
      spark = get_spark_session(project, region, session_template)
      session_startup_ms = (
          None
          if session_was_cached
          else int((time.monotonic() - startup_started) * 1000)
      )
      session_id = extract_session_id(spark)
      session_name = (
          f'projects/{project}/locations/{region}/sessions/{session_id}'
          if session_id
          else None
      )

      statements = split_statements(query) if query else []
      if not statements:
        send_json({
            'id': req_id,
            'sessionId': session_id,
            'error': 'No query provided',
        })
        return

      execution_start_ms = int(time.time() * 1000)
      send_json({
          'id': req_id,
          'status': 'executing',
          'executionStartTime': execution_start_ms,
          'sessionStartupMs': session_startup_ms,
          'sessionName': session_name,
      })

      results = execute_statements(spark, statements)

      # Transparent retry for inactive session on the first statement
      if (
          results
          and 'error' in results[0]
          and is_session_inactive_error(results[0]['error'])
      ):
        reset_session()

        print(
            'Spark session was inactive. Retrying with a fresh session...',
            file=sys.stderr,
        )
        startup_started = time.monotonic()
        spark = get_spark_session(project, region, session_template)
        session_startup_ms = (session_startup_ms or 0) + int(
            (time.monotonic() - startup_started) * 1000
        )
        session_id = extract_session_id(spark)
        session_name = (
            f'projects/{project}/locations/{region}/sessions/{session_id}'
            if session_id
            else None
        )

        execution_start_ms = int(time.time() * 1000)
        send_json({
            'id': req_id,
            'status': 'executing',
            'executionStartTime': execution_start_ms,
            'sessionStartupMs': session_startup_ms,
            'sessionName': session_name,
        })

        results = execute_statements(spark, statements)

      send_json({
          'id': req_id,
          'results': results,
          'sessionId': session_id,
          'executionStartTime': execution_start_ms,
          'sessionStartupMs': session_startup_ms,
          'sessionName': session_name,
      })
    except Exception as e:  # pylint: disable=broad-exception-caught
      send_json({
          'id': req_id,
          'error': str(e),
          'trace': traceback.format_exc(),
      })

  for line in sys.stdin:
    process_line(line)


if __name__ == '__main__':
  main()
