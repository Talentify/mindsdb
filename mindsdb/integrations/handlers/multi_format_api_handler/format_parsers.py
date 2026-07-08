"""
Format detection and parsing utilities for multiple data formats.
Supports JSON, XML, and CSV content from web APIs/pages.
"""

import io
import json
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, Union
import pandas as pd
import logging
import re
from html import unescape
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)


def detect_format(response, url: str) -> Optional[str]:
    """
    Detect format from Content-Type header or URL extension.

    Args:
        response: requests.Response object
        url: URL string

    Returns:
        Format string ('json', 'xml', 'csv') or None if cannot detect
    """
    # Try Content-Type header first
    content_type = response.headers.get('Content-Type', '').lower()

    if 'application/json' in content_type or 'application/javascript' in content_type:
        return 'json'
    elif 'application/xml' in content_type or 'text/xml' in content_type:
        return 'xml'
    elif 'text/csv' in content_type:
        return 'csv'
    elif 'text/plain' in content_type:
        # Plain text could be CSV, try to detect
        if ',' in response.text[:1000]:  # Check first 1000 chars
            return 'csv'

    # Fallback to URL extension
    url_lower = url.lower()
    if url_lower.endswith('.json') or '/json' in url_lower:
        return 'json'
    elif url_lower.endswith('.xml') or '/xml' in url_lower or 'feed' in url_lower or 'rss' in url_lower:
        return 'xml'
    elif url_lower.endswith('.csv'):
        return 'csv'

    # Try to auto-detect from content
    content = response.text.strip()
    if content:
        first_char = content[0]
        if first_char in ['{', '[']:
            return 'json'
        elif first_char == '<':
            return 'xml'

    return None


# Common envelope keys that historically wrapped the record array. Kept for
# back-compat so payloads like {"data": [...]} keep exploding as before.
WHITELIST = ['data', 'results', 'items', 'records', 'rows', 'entries']


def _is_object_array(value: Any) -> bool:
    """True if value is a non-empty list whose (sampled) elements are dicts."""
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(item, dict) for item in value[:20])
    )


def _dig(data: Any, path: str) -> Any:
    """Follow a dot-separated path through nested dicts. Returns None on miss."""
    node = data
    for part in path.split('.'):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return None
    return node


def _find_object_arrays(node, prefix: str = '', depth: int = 0, max_depth: int = 3):
    """Recursively collect (dot_path, list) for every list-of-objects reachable
    within `max_depth` levels of nested dicts."""
    found = []
    if isinstance(node, dict) and depth <= max_depth:
        for k, v in node.items():
            path = f'{prefix}.{k}' if prefix else k
            if _is_object_array(v):
                found.append((path, v))
            elif isinstance(v, dict):
                found.extend(_find_object_arrays(v, path, depth + 1, max_depth))
    return found


def _resolve_records(data, record_path: Optional[str] = None):
    """Determine the primary record array to explode into rows.

    Resolution priority:
      1. explicit `record_path` dot-path;
      2. top-level list;
      3. whitelist envelope keys (list-of-objects only);
      4. auto-detect via bounded recursion — one candidate wins, multiple ->
         longest wins with a warning, none -> treat the dict as a single record.

    Returns (records_list_or_None, chosen_top_or_nested_path_or_None).
    """
    if record_path:
        node = _dig(data, record_path)
        if isinstance(node, list):
            return node, record_path
        logger.warning("record_path '%s' did not resolve to a list; auto-detecting", record_path)

    if isinstance(data, list):
        return data, None

    if isinstance(data, dict):
        for key in WHITELIST:
            val = data.get(key)
            # Whitelist keys match a list of objects OR an empty list (the
            # latter preserves the original "{'data': []} -> empty DF" behavior).
            if isinstance(val, list) and (len(val) == 0 or _is_object_array(val)):
                logger.info(f"Extracting list from '{key}' field")
                return val, key
        candidates = _find_object_arrays(data)
        if len(candidates) == 1:
            return candidates[0][1], candidates[0][0]
        if len(candidates) > 1:
            best = max(candidates, key=lambda c: len(c[1]))
            logger.warning(
                "Multiple record arrays %s; using longest '%s'. Set record_path to override.",
                [p for p, _ in candidates], best[0],
            )
            return best[1], best[0]
        # No record array anywhere: the dict itself is a single record.
        return [data], None

    return None, None


def parse_json(content: str, record_path: Optional[str] = None, auto_explode: bool = True) -> pd.DataFrame:
    """
    Parse JSON content and convert to DataFrame.

    Detects the primary record array generically and explodes it into rows,
    reattaching top-level scalar siblings (e.g. status, count) as constant
    columns. Set `auto_explode=False` to keep the legacy single-row shape for
    object payloads, or `record_path` to point at an ambiguous/nested array.

    Args:
        content: JSON string
        record_path: optional dot-path to the record array (overrides detection)
        auto_explode: when False, object payloads normalize to a single row

    Returns:
        pandas DataFrame
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
        raise ValueError(f"Invalid JSON content: {e}")

    if not isinstance(data, (list, dict)):
        # Primitive type, wrap in DataFrame
        return pd.DataFrame({'value': [data]})

    # Legacy single-row behavior for object payloads when explosion is disabled.
    if not auto_explode and isinstance(data, dict):
        return _ensure_scalar_columns(pd.json_normalize(data))

    records, chosen_path = _resolve_records(data, record_path)
    if records is None:
        return pd.DataFrame({'value': [data]})
    if len(records) == 0:
        return pd.DataFrame()

    df = pd.json_normalize(records, sep='.')

    # Reattach top-level scalar siblings (status, count, request_id, ...) as
    # constant columns so envelope metadata is not lost. Skip the array key;
    # prefix `meta_` on name collisions with normalized record columns.
    if isinstance(data, dict) and chosen_path:
        top_key = chosen_path.split('.')[0]
        for k, v in data.items():
            if k == top_key:
                continue
            if v is None or isinstance(v, (str, int, float, bool)):
                df[k if k not in df.columns else f'meta_{k}'] = v

    return _ensure_scalar_columns(df)


def parse_xml(content: str) -> pd.DataFrame:
    """
    Parse XML content and convert to DataFrame.
    Handles common XML structures and RSS/Atom feeds.

    Args:
        content: XML string

    Returns:
        pandas DataFrame
    """
    try:
        root = ET.fromstring(content)
        records = []

        # Handle RSS/Atom feeds
        if root.tag.endswith('rss') or root.tag.endswith('feed'):
            # RSS feed
            for item in root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                record = _xml_element_to_dict(item)
                records.append(record)
        else:
            # Generic XML structure
            # If root has multiple children of the same type, treat them as records
            children = list(root)
            if children:
                # Group by tag name
                tag_counts = {}
                for child in children:
                    tag = child.tag
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1

                # Find the most common tag (likely the record type)
                if tag_counts:
                    most_common_tag = max(tag_counts, key=tag_counts.get)
                    if tag_counts[most_common_tag] > 1:
                        # Multiple elements of same type - treat as records
                        for child in root.findall(f'.//{most_common_tag}'):
                            record = _xml_element_to_dict(child)
                            records.append(record)
                    else:
                        # Different tags - treat each child as a record
                        for child in children:
                            record = _xml_element_to_dict(child)
                            record['_tag'] = child.tag
                            records.append(record)
                else:
                    # No children, convert root element
                    records.append(_xml_element_to_dict(root))
            else:
                # Root has no children, just text content
                records.append({'content': root.text or ''})

        if not records:
            # Empty XML or no parseable structure
            return pd.DataFrame({'root_tag': [root.tag], 'content': [root.text or '']})

        # Use json_normalize to flatten nested dicts into underscore-separated
        # column names (e.g. author_name). Then ensure all values are scalars
        # so DuckDB receives VARCHAR-compatible types, not VARCHAR[].
        df = pd.json_normalize(records, sep='_')
        df = _ensure_scalar_columns(df)

        # Final cleanup: ensure no HTML tags remain in any string columns
        # This is a defensive measure in case HTML tags were not caught during parsing
        cleaned_count = 0
        for col in df.columns:
            if df[col].dtype == 'object':  # String/object columns
                # Only clean values that contain '<' (potential HTML)
                html_mask = df[col].apply(lambda x: pd.notna(x) and isinstance(x, str) and '<' in x)
                if html_mask.any():
                    df.loc[html_mask, col] = df.loc[html_mask, col].apply(lambda x: _clean_cdata_content(str(x)))
                    cleaned_count += html_mask.sum()

        if cleaned_count > 0:
            logger.info(f"Cleaned HTML tags from {cleaned_count} values during DataFrame post-processing")

        return df

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        raise ValueError(f"Invalid XML content: {e}")


def _try_parse_date(text: str) -> Union[str, pd.Timestamp]:
    """
    Attempt to parse text as a date/datetime.

    Args:
        text: Text that might be a date string

    Returns:
        pandas Timestamp if parsing succeeds, original text otherwise
    """
    if not text or len(text) < 8:  # Minimum reasonable date length
        return text

    try:
        # Use dateutil parser which handles many formats
        # Including: RFC 2822, ISO 8601, and common formats
        parsed = date_parser.parse(text, fuzzy=False)
        return pd.Timestamp(parsed)
    except (ValueError, TypeError, date_parser.ParserError):
        # Not a date, return as-is
        return text


def _clean_cdata_content(text: str) -> Union[str, pd.Timestamp]:
    """
    Clean CDATA content by stripping HTML tags and HTML entities, trimming whitespace,
    and attempting to parse dates.

    Args:
        text: Raw text content that may contain HTML tags, entities, or dates

    Returns:
        Cleaned text content, or pandas Timestamp if a date was detected
    """
    if not text:
        return ''

    # Strip leading/trailing whitespace (common in CDATA sections)
    text = text.strip()

    # Extract URL from anchor tag href if present
    # Matches: <a href="URL" ...>...</a> or <a ...href="URL"...>
    href_match = re.search(r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>', text, re.IGNORECASE)
    if href_match:
        logger.debug(f"Extracting URL from anchor tag: {text[:100]}...")
        text = href_match.group(1)  # Extract just the URL from href attribute

    # Remove HTML tags (multiple passes for nested tags)
    # Loop until no more tags are found
    prev_text = None
    while prev_text != text:
        prev_text = text
        text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities (e.g., &amp; -> &, &lt; -> <)
    text = unescape(text)

    # Remove any remaining excessive whitespace
    text = ' '.join(text.split())

    # Try to parse as date
    return _try_parse_date(text)


def _serialize_non_scalar(value: Any) -> Any:
    """
    Convert non-scalar values (lists, dicts) to string representations
    suitable for flat DataFrame columns compatible with DuckDB.

    Args:
        value: Any cell value

    Returns:
        Scalar value (string, number, timestamp, or None)
    """
    if value is None or isinstance(value, (str, int, float, bool, pd.Timestamp)):
        return value
    if isinstance(value, list):
        if len(value) == 0:
            return ''
        if all(isinstance(item, (str, int, float)) for item in value):
            return ', '.join(str(item) for item in value)
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def _ensure_scalar_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all DataFrame columns contain DuckDB-compatible values.

    Two passes per object column:
      1. Serialize any list/dict cells to strings.
      2. If the column still mixes heterogeneous scalar types
         (e.g. str + float), coerce every non-null cell to str so
         DuckDB does not crash converting the dataframe to Arrow.

    Args:
        df: DataFrame that may contain non-scalar or mixed-type cells

    Returns:
        DataFrame with DuckDB-friendly columns
    """
    for col in df.columns:
        if df[col].dtype != 'object':
            continue

        has_non_scalar = df[col].apply(
            lambda x: isinstance(x, (list, dict))
        ).any()
        if has_non_scalar:
            logger.debug(f"Converting non-scalar values in column '{col}' to strings")
            df[col] = df[col].apply(_serialize_non_scalar)

        non_null = df[col].dropna()
        if non_null.empty:
            continue
        type_set = {type(v) for v in non_null}
        type_set.discard(type(None))
        if len(type_set) > 1:
            logger.debug(f"Coercing mixed-type column '{col}' to string ({type_set})")
            df[col] = df[col].apply(lambda x: x if pd.isna(x) else str(x))
    return df


def _xml_element_to_dict(element: ET.Element) -> Dict[str, Any]:
    """
    Convert XML element to dictionary.

    Args:
        element: XML Element

    Returns:
        Dictionary representation
    """
    result = {}

    # Add attributes
    if element.attrib:
        for key, value in element.attrib.items():
            result[f'@{key}'] = value

    # Add text content
    if element.text and element.text.strip():
        result['text'] = _clean_cdata_content(element.text)

    # Add child elements
    for child in element:
        # Remove namespace from tag
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag

        if len(list(child)) == 0:
            # Leaf node - just get text and clean it
            result[tag] = _clean_cdata_content(child.text or '')
        else:
            # Has children - recursively convert
            child_dict = _xml_element_to_dict(child)
            if tag in result:
                # Duplicate tag - convert to list
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict

    return result


def parse_csv(content: str) -> pd.DataFrame:
    """
    Parse CSV content and convert to DataFrame.

    Args:
        content: CSV string

    Returns:
        pandas DataFrame
    """
    try:
        # Use StringIO to read CSV from string
        return pd.read_csv(io.StringIO(content))
    except Exception as e:
        logger.error(f"CSV parsing error: {e}")
        raise ValueError(f"Invalid CSV content: {e}")


def parse_response(response, url: str, record_path: Optional[str] = None, auto_explode: bool = True) -> pd.DataFrame:
    """
    Auto-detect format and parse response to DataFrame.

    Args:
        response: requests.Response object
        url: URL string
        record_path: optional dot-path to the record array (JSON only)
        auto_explode: when False, JSON object payloads normalize to a single row

    Returns:
        pandas DataFrame
    """
    format_type = detect_format(response, url)

    if format_type == 'json':
        logger.info("Detected JSON format")
        return parse_json(response.text, record_path, auto_explode)
    elif format_type == 'xml':
        logger.info("Detected XML format")
        return parse_xml(response.text)
    elif format_type == 'csv':
        logger.info("Detected CSV format")
        return parse_csv(response.text)
    else:
        # Try JSON as default fallback
        logger.warning(f"Could not detect format, trying JSON as fallback")
        try:
            return parse_json(response.text, record_path, auto_explode)
        except ValueError:
            # Try XML as second fallback
            try:
                logger.warning("JSON failed, trying XML as fallback")
                return parse_xml(response.text)
            except ValueError:
                raise ValueError(
                    f"Unable to detect or parse format for URL: {url}. "
                    f"Content-Type: {response.headers.get('Content-Type', 'unknown')}"
                )
