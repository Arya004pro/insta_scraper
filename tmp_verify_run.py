import csv
import json
import os
import re
import time
import urllib.request
import urllib.error

BASE = 'http://127.0.0.1:8000'
PROFILE = 'https://www.instagram.com/indriyajewels/'
ATTEMPTS = 3
POLL_INTERVAL = 5
POLL_TIMEOUT = 240
ART_KEYS = ['master_summary_csv', 'posts_csv', 'reels_csv']


def extract_run_id(obj):
    if isinstance(obj, dict):
        for key in ('run_id', 'id'):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        for v in obj.values():
            rid = extract_run_id(v)
            if rid:
                return rid
    elif isinstance(obj, list):
        for item in obj:
            rid = extract_run_id(item)
            if rid:
                return rid
    return None


def http_json(method, path, payload=None, timeout=30):
    url = BASE + path
    data = None
    headers = {'Accept': 'application/json'}
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
            if not raw.strip():
                return {}
            try:
                return json.loads(raw)
            except Exception:
                return {'raw': raw}
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        try:
            parsed = json.loads(body) if body else {}
        except Exception:
            parsed = {'raw': body}
        raise RuntimeError(f'HTTP {e.code}: {parsed}')


def start_run_ui():
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        return None, f'playwright_import_failed: {e}'

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(BASE + '/', wait_until='domcontentloaded', timeout=30000)
            page.fill('#urlInput', PROFILE)
            page.click('#startBtn')
            page.wait_for_timeout(2500)

            candidates = [page.url]
            text = page.evaluate("() => document.body ? document.body.innerText : ''")
            candidates.append(text)
            ls_dump = page.evaluate("() => { const o={}; for (let i=0;i<localStorage.length;i++){ const k=localStorage.key(i); o[k]=localStorage.getItem(k);} return JSON.stringify(o); }")
            candidates.append(ls_dump)
            browser.close()

        for blob in candidates:
            if not blob:
                continue
            m = re.search(r'([0-9a-fA-F]{8}-[0-9a-fA-F-]{8,})', blob)
            if m:
                return m.group(1), None
            m2 = re.search(r'run[_\s-]?id[^A-Za-z0-9-]*([A-Za-z0-9-]{8,})', blob, re.IGNORECASE)
            if m2:
                return m2.group(1), None
        return None, 'ui_run_id_not_found'
    except Exception as e:
        return None, f'playwright_ui_failed: {e}'


def start_run_api():
    payload = {'profile_url': PROFILE, 'url': PROFILE}
    resp = http_json('POST', '/v1/runs/start', payload=payload, timeout=60)
    rid = extract_run_id(resp)
    if not rid:
        raise RuntimeError(f'run_id_not_found_in_response: {resp}')
    return rid


def poll_run(run_id):
    deadline = time.time() + POLL_TIMEOUT
    last = {}
    while time.time() < deadline:
        try:
            last = http_json('GET', f'/v1/runs/{run_id}', timeout=30)
        except Exception as e:
            last = {'status': 'poll_error', 'error_message': str(e)}
        status = str(last.get('status', '')).lower()
        if status in {'completed', 'success', 'failed', 'error', 'cancelled', 'done'}:
            return last, False
        time.sleep(POLL_INTERVAL)
    return last, True


def resolve_artifacts(report_obj):
    if isinstance(report_obj, dict):
        if isinstance(report_obj.get('artifacts'), dict):
            return report_obj['artifacts']
        for v in report_obj.values():
            arts = resolve_artifacts(v)
            if arts:
                return arts
    elif isinstance(report_obj, list):
        for item in report_obj:
            arts = resolve_artifacts(item)
            if arts:
                return arts
    return {}


def normalize_path(val):
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        for k in ('path', 'file', 'filepath', 'csv_path', 'value'):
            if isinstance(val.get(k), str) and val.get(k).strip():
                return val.get(k).strip()
    return None


def csv_stats(path_like):
    path = normalize_path(path_like)
    info = {'path': path, 'exists': False, 'data_row_count': 0, 'non_empty_value_cells': 0}
    if not path:
        return info
    full_path = path
    if not os.path.isabs(full_path):
        full_path = os.path.abspath(full_path)
    info['path'] = full_path
    if not os.path.exists(full_path):
        return info
    info['exists'] = True
    try:
        with open(full_path, 'r', encoding='utf-8', errors='replace', newline='') as f:
            rows = list(csv.reader(f))
        data_rows = rows[1:] if len(rows) > 1 else []
        info['data_row_count'] = len(data_rows)
        info['non_empty_value_cells'] = sum(1 for row in data_rows for cell in row if str(cell).strip() != '')
    except Exception:
        pass
    return info


attempt_summaries = []
final_success = False
failed_files = []

for attempt in range(1, ATTEMPTS + 1):
    run_id = None
    start_method = 'ui_playwright'
    ui_error = None

    run_id, ui_error = start_run_ui()
    if not run_id:
        start_method = 'api_fallback'
        try:
            run_id = start_run_api()
        except Exception as e:
            summary = {
                'attempt': attempt,
                'run_id': None,
                'start_method': start_method,
                'status': 'start_failed',
                'error_code': 'START_FAILED',
                'error_message': str(e),
                'ui_error': ui_error,
                'files': {k: csv_stats(None) for k in ART_KEYS},
                'success_condition': False,
            }
            attempt_summaries.append(summary)
            print(json.dumps({'attempt_summary': summary}, ensure_ascii=False))
            continue

    run_state, timed_out = poll_run(run_id)
    report = {}
    report_err = None
    try:
        report = http_json('GET', f'/v1/runs/{run_id}/report', timeout=60)
    except Exception as e:
        report_err = str(e)

    artifacts = resolve_artifacts(report)
    file_stats = {k: csv_stats(artifacts.get(k)) for k in ART_KEYS}

    status = run_state.get('status', 'unknown')
    error_code = run_state.get('error_code') or (report.get('error_code') if isinstance(report, dict) else None)
    error_message = run_state.get('error_message') or (report.get('error_message') if isinstance(report, dict) else None)
    if timed_out:
        error_code = error_code or 'POLL_TIMEOUT'
        error_message = error_message or 'Run polling timed out'

    success_condition = all(file_stats[k]['data_row_count'] > 0 and file_stats[k]['non_empty_value_cells'] > 0 for k in ART_KEYS)

    summary = {
        'attempt': attempt,
        'run_id': run_id,
        'start_method': start_method,
        'status': status,
        'error_code': error_code,
        'error_message': error_message,
        'ui_error': ui_error,
        'report_error': report_err,
        'files': file_stats,
        'success_condition': success_condition,
    }
    attempt_summaries.append(summary)
    print(json.dumps({'attempt_summary': summary}, ensure_ascii=False))

    if success_condition:
        final_success = True
        break

if attempt_summaries:
    last_files = attempt_summaries[-1].get('files', {})
    for k in ART_KEYS:
        st = last_files.get(k, {})
        if not (st.get('data_row_count', 0) > 0 and st.get('non_empty_value_cells', 0) > 0):
            failed_files.append(k)

final_obj = {
    'final_verdict': 'success' if final_success else 'failure',
    'attempt_count': len(attempt_summaries),
    'failed_files_if_any': failed_files,
    'attempt_summaries': attempt_summaries,
}
print(json.dumps({'final_summary': final_obj}, ensure_ascii=False))
