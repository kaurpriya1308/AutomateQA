import streamlit as st
import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Page configuration
st.set_page_config(
    page_title="URL Audit Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
    <style>
    .main { padding: 2rem; }
    .stTextArea textarea {
        font-family: 'Courier New', monospace;
        font-size: 12px;
    }
    .url-text {
        word-wrap: break-word;
        word-break: break-all;
        white-space: pre-wrap;
        font-family: 'Courier New', monospace;
        font-size: 11px;
        background-color: #f0f2f6;
        padding: 8px;
        border-radius: 4px;
        margin: 5px 0;
    }
    .issue-box {
        border-left: 4px solid #ff4b4b;
        padding: 15px;
        margin: 10px 0;
        background-color: #fff5f5;
        border-radius: 4px;
    }
    .success-box {
        border-left: 4px solid #00c853;
        padding: 15px;
        margin: 10px 0;
        background-color: #f1f8f4;
        border-radius: 4px;
    }
    .missing-url-box {
        border-left: 4px solid #ff9800;
        padding: 12px;
        margin: 8px 0;
        background-color: #fff8e1;
        border-radius: 4px;
    }
    .found-url-box {
        border-left: 4px solid #2196f3;
        padding: 12px;
        margin: 8px 0;
        background-color: #e3f2fd;
        border-radius: 4px;
    }
    .crawl-progress {
        font-family: 'Courier New', monospace;
        font-size: 11px;
        color: #666;
    }
    /* Make validation table compact */
    .validation-table {
        font-size: 12px;
    }
    .validation-table td {
        padding: 4px 8px !important;
        vertical-align: middle !important;
    }
    div[data-testid="stDataEditor"] {
        font-size: 12px;
    }
    </style>
""", unsafe_allow_html=True)


# =============================================================================
# CONCURRENT DOMAIN CRAWLER - 50 THREADS, DEPTH 10
# =============================================================================
class ConcurrentDomainCrawler:
    """
    Multi-threaded BFS crawler.
    - Extracts ALL domains from after_save_pageurls
    - Crawls each domain to depth 10
    - Uses 50 concurrent threads for speed
    """

    EXCLUDED_EXTENSIONS = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.7z',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico', '.webp',
        '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.wav',
        '.css', '.js', '.woff', '.woff2', '.ttf', '.eot', '.otf',
        '.exe', '.dmg', '.msi', '.apk',
    }

    EXCLUDED_PATH_PATTERNS = [
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/',
        r'/assets/', r'/static/', r'/images/', r'/img/',
        r'/fonts/', r'/css/', r'/js/',
        r'javascript:', r'mailto:', r'tel:',
        r'/cdn-cgi/', r'/feed/', r'/rss/',
        r'/login', r'/logout', r'/signup', r'/register',
        r'/cart', r'/checkout', r'/account',
        r'/page/\d+', r'\?replytocom=',
        r'/xmlrpc\.php', r'/wp-json/',
    ]

    IR_CONTENT_PATTERNS = [
        r'investor', r'annual.?report', r'quarterly', r'financial',
        r'press.?release', r'news', r'event', r'presentation',
        r'earning', r'sec.?filing', r'proxy', r'governance',
        r'board', r'shareholder', r'dividend', r'stock',
        r'sustainability', r'esg', r'csr', r'responsibility',
        r'report', r'document', r'filing', r'archive',
        r'result', r'disclosure', r'regulatory',
        r'agm', r'meeting', r'webcast', r'transcript',
    ]

    def __init__(self, max_depth=10, max_pages_per_domain=1000,
                 max_workers=50, timeout=10, delay=0.1):
        self.max_depth = max_depth
        self.max_pages_per_domain = max_pages_per_domain
        self.max_workers = max_workers
        self.timeout = timeout
        self.delay = delay

        # Thread-safe counters
        self._lock = threading.Lock()
        self._pages_crawled = 0
        self._total_discovered = 0
        self._current_depth = 0

    def _make_session(self):
        """Create a requests session per thread."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        session.max_redirects = 5
        return session

    def _is_valid_url(self, url, allowed_domains):
        """Check if URL belongs to allowed domains and is not excluded."""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or parsed.scheme not in ('http', 'https'):
                return False
            if not parsed.netloc:
                return False

            url_domain = parsed.netloc.lower().replace('www.', '')
            if url_domain not in allowed_domains:
                return False

            path_lower = parsed.path.lower()
            for ext in self.EXCLUDED_EXTENSIONS:
                if path_lower.endswith(ext):
                    return False

            full_url_lower = url.lower()
            for pattern in self.EXCLUDED_PATH_PATTERNS:
                if re.search(pattern, full_url_lower):
                    return False

            return True
        except Exception:
            return False

    def _normalize_url(self, url):
        """Normalize URL for deduplication."""
        try:
            parsed = urlparse(url)
            normalized = parsed._replace(fragment='')
            path = normalized.path.rstrip('/')
            if not path:
                path = '/'
            normalized = normalized._replace(path=path)
            # Remove common tracking params
            query = normalized.query
            if query:
                clean_params = []
                for param in query.split('&'):
                    key = param.split('=')[0].lower()
                    if key not in ('utm_source', 'utm_medium', 'utm_campaign',
                                   'utm_term', 'utm_content', 'fbclid', 'gclid'):
                        clean_params.append(param)
                query = '&'.join(clean_params)
            normalized = normalized._replace(query=query)
            return normalized.geturl()
        except Exception:
            return url

    def _fetch_page(self, url, session):
        """Fetch a single page and extract links."""
        links = []
        try:
            time.sleep(self.delay)
            response = session.get(url, timeout=self.timeout, allow_redirects=True)
            if response.status_code != 200:
                return links
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                return links

            soup = BeautifulSoup(response.text, 'html.parser')
            for tag in soup.find_all('a', href=True):
                href = tag['href'].strip()
                if href and not href.startswith('#') and not href.startswith('javascript:'):
                    absolute_url = urljoin(url, href)
                    links.append(absolute_url)
        except Exception:
            pass
        return links

    def _crawl_batch(self, urls_with_depth, allowed_domains, visited, session):
        """Crawl a batch of URLs concurrently, return new discovered URLs with depth."""
        new_urls = []

        for url, depth in urls_with_depth:
            if depth > self.max_depth:
                continue

            with self._lock:
                if url in visited:
                    continue
                visited.add(url)
                self._pages_crawled += 1
                self._current_depth = max(self._current_depth, depth)

                if self._pages_crawled > self.max_pages_per_domain:
                    return new_urls

            links = self._fetch_page(url, session)

            for link in links:
                normalized = self._normalize_url(link)
                if self._is_valid_url(normalized, allowed_domains):
                    with self._lock:
                        if normalized not in visited:
                            self._total_discovered += 1
                            new_urls.append((normalized, depth + 1))

        return new_urls

    def crawl_all_domains(self, after_urls, progress_callback=None):
        """
        Main crawl method.
        1. Extract ALL domains from after_urls
        2. Use all HTTP URLs as seed URLs
        3. BFS crawl with 50 concurrent workers to depth 10
        4. Return dict of {discovered_url: {"seed": seed_url, "depth": depth, "domain": domain}}
        """
        # Extract all HTTP seed URLs and their domains
        seed_urls = []
        allowed_domains = set()
        seed_map = {}  # url -> seed_url that led to it

        for url in after_urls:
            if not isinstance(url, str):
                continue
            url = url.strip()
            if url.startswith('http'):
                parsed = urlparse(url)
                domain = parsed.netloc.lower().replace('www.', '')
                allowed_domains.add(domain)
                norm = self._normalize_url(url)
                seed_urls.append(norm)
                seed_map[norm] = norm  # seed is itself

        if not seed_urls or not allowed_domains:
            return {}

        # Reset counters
        self._pages_crawled = 0
        self._total_discovered = len(seed_urls)
        self._current_depth = 0

        visited = set()
        all_discovered = {}  # url -> {seed, depth, domain}

        # Initialize with seed URLs at depth 0
        current_level = [(url, 0) for url in seed_urls]
        for url in seed_urls:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace('www.', '')
            all_discovered[url] = {
                "seed": url,
                "depth": 0,
                "domain": domain
            }

        if progress_callback:
            progress_callback(0, len(seed_urls), len(all_discovered), 0,
                              f"Starting with {len(seed_urls)} seed URLs across "
                              f"{len(allowed_domains)} domain(s)")

        # BFS with concurrent workers
        for depth_level in range(self.max_depth + 1):
            if not current_level:
                break

            if self._pages_crawled >= self.max_pages_per_domain:
                break

            # Split current level into batches for workers
            batch_size = max(1, len(current_level) // self.max_workers)
            batches = []
            for i in range(0, len(current_level), batch_size):
                batches.append(current_level[i:i + batch_size])

            next_level = []

            with ThreadPoolExecutor(max_workers=min(self.max_workers, len(batches))) as executor:
                futures = []
                for batch in batches:
                    session = self._make_session()
                    future = executor.submit(
                        self._crawl_batch, batch, allowed_domains, visited, session
                    )
                    futures.append(future)

                for future in as_completed(futures):
                    try:
                        new_urls = future.result(timeout=120)
                        for new_url, new_depth in new_urls:
                            if new_url not in all_discovered:
                                parsed = urlparse(new_url)
                                domain = parsed.netloc.lower().replace('www.', '')

                                # Find which seed URL led here
                                parent_seed = None
                                for s_url in seed_urls:
                                    s_parsed = urlparse(s_url)
                                    s_domain = s_parsed.netloc.lower().replace('www.', '')
                                    if domain == s_domain:
                                        parent_seed = s_url
                                        break
                                if not parent_seed:
                                    parent_seed = seed_urls[0]

                                all_discovered[new_url] = {
                                    "seed": parent_seed,
                                    "depth": new_depth,
                                    "domain": domain
                                }
                                next_level.append((new_url, new_depth))
                    except Exception:
                        continue

            if progress_callback:
                progress_callback(
                    self._pages_crawled,
                    len(next_level),
                    len(all_discovered),
                    depth_level,
                    f"Depth {depth_level} complete | "
                    f"Crawled: {self._pages_crawled} | "
                    f"Next level: {len(next_level)} URLs"
                )

            current_level = next_level

        return all_discovered

    @staticmethod
    def is_ir_relevant(url):
        url_lower = url.lower()
        for pattern in ConcurrentDomainCrawler.IR_CONTENT_PATTERNS:
            if re.search(pattern, url_lower):
                return True
        return False


# =============================================================================
# URL MATCHING UTILITIES
# =============================================================================
class URLMatcher:

    @staticmethod
    def extract_all_domains(urls):
        domains = set()
        for url in urls:
            if isinstance(url, str) and url.strip().startswith('http'):
                parsed = urlparse(url.strip())
                domain = parsed.netloc.lower().replace('www.', '')
                if domain:
                    domains.add(domain)
        return domains

    @staticmethod
    def extract_http_urls(urls):
        return [url.strip() for url in urls
                if isinstance(url, str) and url.strip().startswith('http')]

    @staticmethod
    def extract_regex_patterns(urls):
        patterns = []
        for url in urls:
            if not isinstance(url, str):
                continue
            url = url.strip()
            if re.match(r'^(ev|cp|df|if):', url, re.IGNORECASE):
                patterns.append(url)
        return patterns

    @staticmethod
    def url_matches_exact(discovered_url, after_urls):
        norm = discovered_url.rstrip('/').replace('://www.', '://')
        for after_url in after_urls:
            if not isinstance(after_url, str) or not after_url.startswith('http'):
                continue
            norm_after = after_url.strip().rstrip('/').replace('://www.', '://')
            if norm == norm_after:
                return True
        return False

    @staticmethod
    def url_matches_regex_pattern(discovered_url, regex_patterns):
        parsed = urlparse(discovered_url)
        url_path = parsed.path
        for pattern_str in regex_patterns:
            match = re.match(r'^(ev|cp|df|if):(.*)', pattern_str, re.IGNORECASE)
            if not match:
                continue
            regex_part = match.group(2)
            if not regex_part:
                continue
            try:
                if re.search(regex_part, url_path):
                    return pattern_str
                if re.search(regex_part, discovered_url):
                    return pattern_str
            except re.error:
                continue
        return None

    @staticmethod
    def classify_discovered_url(discovered_url, after_urls, regex_patterns):
        if URLMatcher.url_matches_exact(discovered_url, after_urls):
            return True, "Already added"
        matching = URLMatcher.url_matches_regex_pattern(discovered_url, regex_patterns)
        if matching:
            return True, f"Sub page - scraped with ev/cp regex"
        return False, "Potentially missing"


# =============================================================================
# URL AUDITOR CLASS
# =============================================================================
class URLAuditor:

    TEMPLATE_INDICATORS = [
        r'\$\{', r'\{miny', r'\{epp', r'\{xpath', r'\{onclick',
        r'\{json[=_]', r'\{js_', r'\{jsarg', r'\{baseurl', r'\{window',
        r'^cp:', r'^ev:', r'^df:', r'^if:', r'wd:', r'curl:',
        r'json:xhr:', r'json:curl:', r'appid',
    ]

    @staticmethod
    def clean_json_input(json_text):
        json_text = json_text.strip()
        lines = json_text.split('\n')
        cleaned_lines = [re.sub(r'^[\s\-]+', '', line) for line in lines if line.strip()]
        json_text = '\n'.join(cleaned_lines)

        if not json_text.startswith('{') and not json_text.startswith('['):
            match = re.search(r'[{\[]', json_text)
            if match:
                json_text = json_text[match.start():]

        if '{' in json_text:
            start = json_text.index('{')
            brace_count = 0
            end = start
            for i in range(start, len(json_text)):
                if json_text[i] == '{':
                    brace_count += 1
                elif json_text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end = i + 1
                        break
            json_text = json_text[start:end]
        return json_text

    @staticmethod
    def parse_json(json_text):
        cleaned = URLAuditor.clean_json_input(json_text)
        errors = []
        for attempt, fn in [
            ("Direct", lambda t: json.loads(t)),
            ("Non-strict", lambda t: json.loads(t, strict=False)),
            ("Fixed", lambda t: json.loads(
                re.sub(r',\s*([}\]])', r'\1',
                        re.sub(r'"\s*\n\s*"', '",\n"', t).replace("'", '"'))
            )),
        ]:
            try:
                return fn(cleaned), None
            except json.JSONDecodeError as e:
                errors.append(f"{attempt}: {e.msg} at line {e.lineno}")
        return None, errors

    @staticmethod
    def urls_contain_templates(urls):
        for url in urls:
            if not isinstance(url, str):
                continue
            for p in URLAuditor.TEMPLATE_INDICATORS:
                if re.search(p, url, re.IGNORECASE):
                    return True
        return False

    @staticmethod
    def check_miny(urls):
        issues = []
        pat = r"\$\{y\}|\$\{ym1\}|\$\{yp1\}|\$\{y2\}|\$\{ym2\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{miny", url)):
                if not bool(re.search(r"\$\{miny=\:\d{4}\}", url)) or not bool(re.search(pat, url)):
                    issues.append({"type": "MINY Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_epp(urls):
        issues = []
        pat = r"\$\{p\}|\$\{pm1\}|\$\{pp1\}|\$\{stm1\}|\$\{st\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{epp", url)):
                if not bool(re.search(r"\$\{epp=\:\d{1,2}\}", url)) or not bool(re.search(pat, url)):
                    issues.append({"type": "EPP Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_xpath(urls):
        issues = []
        pat = (
            r"\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.*\};\>\}.*xml"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};;;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\}\>\}"
            r"|\$\{xpath=\:\<\{//tr\};\{td\[4\]\};\{td\[2\]\};\{td\[1\]\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};\{.*}.*\>\}"
            r"|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};;.*\>\}"
        )
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{xpath", url)) and not bool(re.search(pat, url)):
                issues.append({"type": "XPATH Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_onclick(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{onclick", url)) and not bool(re.search(r'\$\{onclick_var=\:\".*\"\}', url)):
                issues.append({"type": "ONCLICK Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_jsarg(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"jsarg", url)) and not bool(re.search(r'\$\{jsarg=\:\d\}', url)):
                issues.append({"type": "JSARG Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_json_template(urls):
        issues = []
        jp = (
            r"\$\{json=\:\<\{cp\:\:|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\"\>\}"
            r"|\$\{json=\:\<\".*\";\".*\"\>\}|\$\{json=\:\<\".*\"; \".*\"\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";\>\}|\$\{json=\:\<\{tr\:\:"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\>\}|\$\{json=\:\<\".*\";\".*\";;\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";;;\>\}|GetFinancialReportListResult"
            r"|GetPresentationListResult|GetEventListResult"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\".*;\".*\";.*\>\}"
        )
        mp = (r'json\:xhr\:|json\:uepost\:xhr\:|json\:jspost\:xhr\:'
              r'|json\:curl\:xhr\:|json\:curl\:|appid|json\:\$\{url\}|json\:xhr\:uepost\:')
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{json=", url)):
                if not bool(re.search(jp, url)) or not bool(re.search(mp, url)):
                    issues.append({"type": "JSON Template Incorrect", "url_index": idx, "url": url})
            elif bool(re.search(r"\{json_", url)):
                if not bool(re.search(r"\$\{json_data_load=\:1\}|\$\{json_data_load=\:True\}", url)):
                    issues.append({"type": "JSON Data Load Template Incorrect", "url_index": idx, "url": url})
            elif bool(re.search(r"\{js_", url)):
                if not bool(re.search(r"\$\{js_json=\:1\}", url)):
                    issues.append({"type": "JS JSON Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_baseurl(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{baseurl", url)):
                if not bool(re.search(r"\$\{baseurl=\:\".*\"\}|\$\{full_baseurl=\:True\}", url)):
                    issues.append({"type": "BASEURL Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_windowflag(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if bool(re.search(r"\{window", url)):
                if not bool(re.search(r"\$\{window_flag_regex=\:\".*\"\}|\$\{window_flag=\:True\}", url)):
                    issues.append({"type": "Window Flag Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_regex(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str) or len(url) < 4: continue
            if bool(re.search(r"^ev|^df|^cp|^if", url)):
                has_upper = bool(re.search(r"[A-Z]", url))
                has_escaped = bool(re.search(r"\\[A-Z]|A\-Z", url))
                if len(url) >= 11 and has_upper and not has_escaped:
                    issues.append({"type": "Regex Incorrect - Uppercase not escaped", "url_index": idx, "url": url})
                elif len(url) >= 11 and url[2] != ":":
                    issues.append({"type": "Regex Incorrect - Missing colon", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_http(urls):
        issues = []
        skip_pat = '|'.join([r'^df', r'^if', r'^ev', r'^cp'])
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str) or len(url) <= 5: continue
            if bool(re.search(skip_pat, url, re.IGNORECASE)): continue
            has_http = "http" in url.lower()
            has_multi = bool(re.search(r"http.*http", url, re.IGNORECASE))
            if has_multi:
                cleaned = re.sub(r'\$\{baseurl\=\:\"http', '', url, count=1, flags=re.IGNORECASE)
                has_multi = bool(re.search(r"http.*http", cleaned, re.IGNORECASE))
            has_nl = bool(re.search(r"\n", url))
            if not has_http:
                issues.append({"type": "Missing HTTP/HTTPS", "url_index": idx, "url": url})
            elif has_multi:
                issues.append({"type": "Multiple HTTP in URL", "url_index": idx, "url": url})
            elif has_nl:
                issues.append({"type": "Newline in URL", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_brackets(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            if url.count("{") != url.count("}"):
                issues.append({
                    "type": "Mismatched Brackets {}",
                    "url_index": idx, "url": url,
                    "details": f"Open: {url.count('{')}, Close: {url.count('}')}"
                })
        return issues

    @staticmethod
    def check_duplicates(urls):
        issues = []
        url_map = {}
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str): continue
            c = url.strip()
            if len(c) <= 3 or c.lower() in ('nan', 'none', 'null', 'n/a', ''): continue
            url_map.setdefault(c, []).append(idx)
        for url, indices in url_map.items():
            if len(indices) > 1:
                issues.append({"type": "Duplicate URL", "url_indices": indices,
                               "url": url, "occurrences": len(indices)})
        return issues

    @staticmethod
    def check_metadata(data):
        issues = []
        agent_status = str(data.get("status", "") or "").strip().lower()
        case_type = str(data.get("case_type", "") or "").strip().lower()
        project = str(data.get("project", "") or "").strip()
        research_status = str(data.get("research_status", "") or "").strip().lower()
        issue_area = str(data.get("issue_area", "") or "").strip()
        final_status_val = str(data.get("final_status", "") or "").strip()
        irsp_provider = str(data.get("irsp_provider", "") or "").strip()
        after_urls = data.get("after_save_pageurls", [])

        is_active = bool(re.search(r"verified$|manual|escalated_to_technology_team", agent_status))
        has_active = bool(re.search(r"verified|manual|escalated|website_is_down|internal_review", agent_status))

        if is_active and not case_type:
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "No Case Type with Verified/Manual/Escalated Agent status"})

        if any("curl:" in str(u) for u in after_urls if isinstance(u, str)) and case_type != "cookie_case":
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "URLs contain 'curl:' but case_type is not 'cookie_case'"})

        if any("s3.amazonaws.com" in str(u) for u in after_urls if isinstance(u, str)):
            if case_type != "manual_solution_webpage_generated":
                issues.append({"type": "Metadata Error", "field": "case_type",
                               "message": "S3 URL found but case_type is not 'manual_solution_webpage_generated'"})

        if research_status == "not_fixed" and project != "QA":
            issues.append({"type": "Metadata Error", "field": "research_status / project",
                           "message": "Not fixed status but project not QA"})

        if (not issue_area and agent_status not in ["internal_review", "miscellaneous"]
                and project not in ["New Ticker", "QA"]):
            issues.append({"type": "Metadata Error", "field": "issue_area",
                           "message": "Issue Area missing for non-covered case"})

        if not after_urls and has_active:
            issues.append({"type": "Metadata Error", "field": "after_save_pageurls",
                           "message": "Case verified/escalated but no URL added"})

        if irsp_provider.lower() == "q4web" and not has_active:
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": "Q4Web added with non-active Agent Status"})

        wd_urls = [u for u in after_urls if isinstance(u, str) and re.search(r"wd:", u)]
        if wd_urls and case_type == "direct":
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "WD in URLs but case type is direct | " + " || ".join(wd_urls[:3])})

        if case_type == "direct" and after_urls and URLAuditor.urls_contain_templates(after_urls):
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "'Direct' case_type but templates found in after_save_pageurls"})

        if is_active:
            if not issue_area:
                issues.append({"type": "Metadata Error", "field": "issue_area",
                               "message": "Issue Area is blank. Must not be empty."})
            if not final_status_val:
                issues.append({"type": "Metadata Error", "field": "final_status",
                               "message": "Final Status is blank. Must not be empty."})

        has_cp = any(isinstance(u, str) and u.strip().startswith("cp:") for u in after_urls)
        if has_cp and irsp_provider:
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": f"'cp:' in URLs but irsp_provider='{irsp_provider}'. Should be blank."})

        first3 = after_urls[:3] if len(after_urls) >= 3 else after_urls
        has_text = any(isinstance(u, str) and u.strip().lower().startswith("text:") for u in first3)
        if has_text and irsp_provider != "Q4Web":
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": f"First 3 URLs have 'text:' but irsp_provider='{irsp_provider}'. Should be 'Q4Web'."})

        return issues

    @classmethod
    def audit_urls(cls, data):
        urls = data.get("after_save_pageurls", [])
        issues = []
        if urls:
            for check in [cls.check_miny, cls.check_epp, cls.check_xpath, cls.check_onclick,
                          cls.check_jsarg, cls.check_json_template, cls.check_baseurl,
                          cls.check_windowflag, cls.check_regex, cls.check_http,
                          cls.check_brackets, cls.check_duplicates]:
                issues.extend(check(urls))
        issues.extend(cls.check_metadata(data))
        return {"status": "Complete", "total_urls": len(urls),
                "issues_found": len(issues), "issues": issues}


def display_url_wrapped(url):
    return f'<div class="url-text">{url}</div>'


# =============================================================================
# MAIN APP
# =============================================================================
def main():
    st.title("🔍 URL Audit Tool")
    st.markdown("---")

    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""
        ### How to use:
        1. **Paste JSON** → Click **Run Audit** for template/metadata checks
        2. Click **Check for Missing URLs** to deep-crawl all domains (50 concurrent threads, depth 10)
        3. Review missing URLs in the table and select validation status

        ### Crawl Details:
        - **Domains**: ALL domains extracted from ALL after_save_pageurls (not just first 5)
        - **Depth**: 10 levels deep (configurable)
        - **Concurrency**: 50 simultaneous requests
        - **Comparison**: Each discovered URL checked against exact matches + ev/cp regex patterns

        ### Validation Options:
        | Option | Meaning |
        |---|---|
        | Already added | Exact URL exists in after_save_pageurls |
        | Added in ticker issue sheet | Tracked in separate sheet |
        | Added with template | Covered via ${miny}, ${epp}, etc. |
        | Sub page - scraped with ev/cp regex | Path matches regex |
        | Added in other module | Covered in a different module |
        | Not relevant / Ignore | Not needed |
        | Needs investigation | Requires review |
        """)

    # Session state
    for key, default in [
        ('audit_results', None), ('audit_data', None),
        ('clear_trigger', False), ('crawl_results', None),
        ('validation_data', None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    # JSON Input
    st.subheader("📝 JSON Input")
    default_val = "" if st.session_state.clear_trigger else st.session_state.get('last_input', '')

    json_input = st.text_area(
        "Paste your JSON data here:", height=300,
        placeholder='{\n  "id": "12345",\n  "status": "verified",\n  "after_save_pageurls": [...]\n}',
        value=default_val, key="json_text_area"
    )

    if json_input and not st.session_state.clear_trigger:
        st.session_state.last_input = json_input
    if st.session_state.clear_trigger:
        st.session_state.clear_trigger = False

    # Buttons
    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        run_btn = st.button("🚀 Run Audit", type="primary", use_container_width=True)
    with c2:
        clear_btn = st.button("🗑️ Clear All", use_container_width=True)
    with c3:
        if st.session_state.audit_results:
            st.download_button("📥 Download Audit",
                               data=json.dumps(st.session_state.audit_results, indent=2),
                               file_name=f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                               mime="application/json", use_container_width=True)

    if clear_btn:
        for k in ['audit_results', 'audit_data', 'crawl_results', 'validation_data']:
            st.session_state[k] = None
        st.session_state.last_input = ""
        st.session_state.clear_trigger = True
        st.rerun()

    # =================================================================
    # RUN AUDIT
    # =================================================================
    if run_btn:
        if not json_input.strip():
            st.warning("⚠️ Paste JSON first!")
        else:
            with st.spinner("🔄 Auditing..."):
                data, errs = URLAuditor.parse_json(json_input)
                if data is None:
                    st.error("❌ JSON parse failed")
                    with st.expander("Errors"):
                        for e in errs:
                            st.text(e)
                else:
                    st.session_state.audit_results = URLAuditor.audit_urls(data)
                    st.session_state.audit_data = data
                    st.session_state.crawl_results = None
                    st.session_state.validation_data = None
                    st.success("✅ Audit done!")

    # =================================================================
    # DISPLAY AUDIT RESULTS
    # =================================================================
    if st.session_state.audit_results:
        st.markdown("---")
        data = st.session_state.audit_data
        results = st.session_state.audit_results

        ticker = data.get("ticker", data.get("as_company_id", "Unknown"))
        cid = data.get("as_company_id", data.get("id", "Unknown"))
        st.header(f"📊 Results: {ticker} ({cid})")
        st.caption(f"Agent Status: **{data.get('status', 'N/A')}**")

        m1, m2, m3 = st.columns(3)
        with m1: st.metric("URLs", results["total_urls"])
        with m2: st.metric("Issues", results["issues_found"])
        with m3: st.metric("Status", "✅ PASS" if results["issues_found"] == 0 else "❌ FAIL")

        st.markdown("---")

        with st.expander("📋 Parsed Fields", expanded=False):
            fc1, fc2 = st.columns(2)
            with fc1:
                for f in ['status', 'case_type', 'project', 'issue_area']:
                    st.write(f"**{f}:** {data.get(f, 'N/A')}")
            with fc2:
                for f in ['final_status', 'irsp_provider', 'research_status', 'verified']:
                    st.write(f"**{f}:** {data.get(f, 'N/A')}")

        if results["issues_found"] == 0:
            st.markdown('<div class="success-box"><h3>✓ No Issues!</h3></div>', unsafe_allow_html=True)
        else:
            st.subheader(f"⚠️ {results['issues_found']} Issues")
            by_type = {}
            for iss in results["issues"]:
                by_type.setdefault(iss["type"], []).append(iss)

            for itype, ilist in by_type.items():
                with st.expander(f"**{itype}** ({len(ilist)})", expanded=True):
                    for i, iss in enumerate(ilist, 1):
                        st.markdown(f"**#{i}:**")
                        if 'url_index' in iss: st.write(f"📍 Index: {iss['url_index']}")
                        if 'url_indices' in iss:
                            st.write(f"📍 Indices: {iss['url_indices']}")
                            st.write(f"🔢 Occurrences: {iss['occurrences']}")
                        if 'field' in iss:
                            st.write(f"🏷️ `{iss['field']}` — {iss['message']}")
                        if 'url' in iss:
                            st.markdown(display_url_wrapped(iss['url']), unsafe_allow_html=True)
                        if 'details' in iss: st.info(iss['details'])
                        if i < len(ilist): st.markdown("---")

            st.table([{"Issue Type": t, "Count": len(l)} for t, l in by_type.items()])

        # =============================================================
        # MISSING URL CHECK
        # =============================================================
        st.markdown("---")
        st.header("🌐 Missing URL Check")

        after_urls = data.get("after_save_pageurls", [])
        all_http = URLMatcher.extract_http_urls(after_urls)
        all_domains = URLMatcher.extract_all_domains(after_urls)

        if not all_http:
            st.warning("⚠️ No HTTP URLs in after_save_pageurls.")
        else:
            st.info(
                f"🔗 **{len(all_http)} HTTP seed URLs** across "
                f"**{len(all_domains)} domain(s)**: {', '.join(sorted(all_domains))}"
            )

            with st.expander("⚙️ Crawl Settings", expanded=False):
                s1, s2, s3, s4 = st.columns(4)
                with s1: depth = st.slider("Depth", 1, 15, 10, key="cd")
                with s2: pages = st.slider("Max Pages/Domain", 100, 2000, 1000, 100, key="cp")
                with s3: workers = st.slider("Concurrent Workers", 10, 100, 50, 5, key="cw")
                with s4: delay = st.slider("Delay (s)", 0.05, 1.0, 0.1, 0.05, key="cdl")
                ir_only = st.checkbox("Show only IR-relevant missing URLs", True, key="iro")

            crawl_btn = st.button("🔎 Check for Missing URLs", type="secondary",
                                  use_container_width=True)

            if crawl_btn:
                crawler = ConcurrentDomainCrawler(
                    max_depth=depth, max_pages_per_domain=pages,
                    max_workers=workers, delay=delay
                )

                prog = st.progress(0)
                stat = st.empty()
                det = st.empty()

                def cb(crawled, queued, discovered, d, msg):
                    prog.progress(min(crawled / pages, 1.0))
                    stat.markdown(
                        f"**Crawled:** {crawled} | **Queue:** {queued} | "
                        f"**Found:** {discovered} | **Depth:** {d}"
                    )
                    det.markdown(f'<span class="crawl-progress">{msg[:120]}</span>',
                                 unsafe_allow_html=True)

                with st.spinner("🕷️ Crawling with 50 threads..."):
                    disc = crawler.crawl_all_domains(after_urls, progress_callback=cb)

                prog.progress(1.0)
                stat.markdown(f"**✅ Done!** Found **{len(disc)}** unique URLs")
                det.empty()

                # Classify
                regex_pats = URLMatcher.extract_regex_patterns(after_urls)
                results_list = []
                for url, info in sorted(disc.items()):
                    covered, reason = URLMatcher.classify_discovered_url(url, after_urls, regex_pats)
                    ir = ConcurrentDomainCrawler.is_ir_relevant(url)
                    results_list.append({
                        "url": url,
                        "seed_url": info["seed"],
                        "domain": info["domain"],
                        "depth": info["depth"],
                        "is_covered": covered,
                        "auto_reason": reason,
                        "is_ir": ir,
                    })

                st.session_state.crawl_results = results_list

                # Build validation table data
                missing = [r for r in results_list if not r["is_covered"]]
                if ir_only:
                    missing = [r for r in missing if r["is_ir"]]

                val_data = []
                for r in missing:
                    val_data.append({
                        "Source Seed URL": r["seed_url"],
                        "Missing URL": r["url"],
                        "Domain": r["domain"],
                        "Depth": r["depth"],
                        "IR Relevant": "Yes" if r["is_ir"] else "No",
                        "Validation": "-- Select --",
                    })
                st.session_state.validation_data = val_data

            # =========================================================
            # DISPLAY CRAWL RESULTS
            # =========================================================
            if st.session_state.crawl_results:
                cr = st.session_state.crawl_results
                covered_list = [r for r in cr if r["is_covered"]]
                uncov_list = [r for r in cr if not r["is_covered"]]
                ir_uncov = [r for r in uncov_list if r["is_ir"]]

                st.markdown("---")
                st.subheader("📊 Crawl Summary")

                x1, x2, x3, x4 = st.columns(4)
                with x1: st.metric("Discovered", len(cr))
                with x2: st.metric("Covered", len(covered_list))
                with x3: st.metric("Missing", len(uncov_list))
                with x4: st.metric("IR Missing", len(ir_uncov))

                # --- MISSING URLs TABLE WITH VALIDATION ---
                st.markdown("### 🔴 Missing URLs — Validation Table")

                if st.session_state.validation_data:
                    vd = st.session_state.validation_data

                    if not vd:
                        st.markdown(
                            '<div class="success-box"><h4>✓ No missing URLs!</h4></div>',
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(f"**{len(vd)} potentially missing URLs found.** "
                                    f"Select validation for each row:")

                        VALIDATION_OPTIONS = [
                            "-- Select --",
                            "Already added",
                            "Added in ticker issue sheet",
                            "Added with template",
                            "Sub page - scraped with ev/cp regex",
                            "Added in other module",
                            "Not relevant / Ignore",
                            "Needs investigation",
                        ]

                        # Table header
                        hdr1, hdr2, hdr3, hdr4, hdr5 = st.columns([3, 4, 1, 1, 3])
                        with hdr1:
                            st.markdown("**Source Seed URL**")
                        with hdr2:
                            st.markdown("**Missing URL**")
                        with hdr3:
                            st.markdown("**Depth**")
                        with hdr4:
                            st.markdown("**IR?**")
                        with hdr5:
                            st.markdown("**Validation**")

                        st.markdown("---")

                        # Table rows
                        for i, row in enumerate(vd):
                            c1, c2, c3, c4, c5 = st.columns([3, 4, 1, 1, 3])

                            with c1:
                                seed_short = row["Source Seed URL"]
                                if len(seed_short) > 50:
                                    seed_short = seed_short[:50] + "..."
                                st.markdown(
                                    f'<a href="{row["Source Seed URL"]}" target="_blank" '
                                    f'title="{row["Source Seed URL"]}">{seed_short}</a>',
                                    unsafe_allow_html=True
                                )

                            with c2:
                                url_short = row["Missing URL"]
                                if len(url_short) > 60:
                                    url_short = url_short[:60] + "..."
                                st.markdown(
                                    f'<a href="{row["Missing URL"]}" target="_blank" '
                                    f'title="{row["Missing URL"]}">{url_short}</a>',
                                    unsafe_allow_html=True
                                )

                            with c3:
                                st.write(str(row["Depth"]))

                            with c4:
                                st.write("🟢" if row["IR Relevant"] == "Yes" else "⚪")

                            with c5:
                                current = row.get("Validation", "-- Select --")
                                idx = VALIDATION_OPTIONS.index(current) if current in VALIDATION_OPTIONS else 0
                                selected = st.selectbox(
                                    f"val_{i}", options=VALIDATION_OPTIONS,
                                    index=idx, key=f"val_select_{i}",
                                    label_visibility="collapsed"
                                )
                                vd[i]["Validation"] = selected

                        # --- VALIDATION SUMMARY ---
                        st.markdown("---")
                        st.markdown("### 📋 Validation Progress")

                        validated = sum(1 for r in vd if r["Validation"] != "-- Select --")
                        total = len(vd)
                        st.progress(validated / total if total > 0 else 0)
                        st.write(f"**{validated}/{total}** URLs validated")

                        summary = {}
                        for r in vd:
                            v = r["Validation"]
                            if v != "-- Select --":
                                summary[v] = summary.get(v, 0) + 1

                        if summary:
                            st.table([{"Status": k, "Count": v}
                                      for k, v in sorted(summary.items())])

                        # Download buttons
                        dc1, dc2 = st.columns(2)
                        with dc1:
                            st.download_button(
                                "📥 Download Missing URLs (JSON)",
                                data=json.dumps(vd, indent=2),
                                file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                mime="application/json", use_container_width=True
                            )
                        with dc2:
                            # CSV download
                            csv_lines = ["Source Seed URL,Missing URL,Domain,Depth,IR Relevant,Validation"]
                            for r in vd:
                                csv_lines.append(
                                    f'"{r["Source Seed URL"]}","{r["Missing URL"]}",'
                                    f'"{r["Domain"]}",{r["Depth"]},{r["IR Relevant"]},'
                                    f'"{r["Validation"]}"'
                                )
                            st.download_button(
                                "📥 Download Missing URLs (CSV)",
                                data="\n".join(csv_lines),
                                file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv", use_container_width=True
                            )

                # --- COVERED URLs ---
                with st.expander(f"🟢 Already Covered URLs ({len(covered_list)})", expanded=False):
                    if covered_list:
                        for i, r in enumerate(covered_list[:200]):
                            st.markdown(
                                f'<div class="found-url-box">'
                                f'<strong>#{i+1}</strong> '
                                f'<a href="{r["url"]}" target="_blank">{r["url"]}</a>'
                                f'<br><em>{r["auto_reason"]} | Depth: {r["depth"]}</em>'
                                f'</div>',
                                unsafe_allow_html=True
                            )
                        if len(covered_list) > 200:
                            st.info(f"Showing first 200 of {len(covered_list)} covered URLs")

    # Footer
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#666;padding:20px;">'
        'URL Audit Tool v3.0 | 50-Thread Concurrent Crawler</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
