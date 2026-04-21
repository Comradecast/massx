from __future__ import annotations

import json
from collections.abc import Iterable

from bs4 import BeautifulSoup, Tag

from .io_utils import normalize_whitespace

CONTENT_SELECTORS = [
    "article",
    "main",
    "[role='main']",
    "[itemprop='articleBody']",
    "[data-testid='story-body']",
    "[data-component='text-block']",
    ".article-body",
    ".article-content",
    ".article__body",
    ".story-body",
    ".story",
    ".entry-content",
    ".post-content",
    ".content-body",
    ".story-content",
    "#main-content",
]

NOISE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "svg",
    "footer",
    "header",
    "nav",
    "aside",
    "form",
    "button",
    ".advertisement",
    ".ad",
    ".promo",
    ".newsletter",
    ".related",
    ".social-share",
]


def _remove_noise(soup: BeautifulSoup) -> None:
    for selector in NOISE_SELECTORS:
        for node in soup.select(selector):
            node.decompose()


def _container_text(container: Tag) -> str:
    parts: list[str] = []
    for node in container.find_all(["p", "h1", "h2", "h3", "li"]):
        text = normalize_whitespace(node.get_text(" ", strip=True))
        if len(text) >= 30:
            parts.append(text)
    return normalize_whitespace(" ".join(parts))


def _extract_json_ld_texts(soup: BeautifulSoup) -> list[str]:
    texts: list[str] = []
    for script in soup.select("script[type='application/ld+json']"):
        payload = script.string or script.get_text(" ", strip=True)
        if not payload:
            continue
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        texts.extend(_walk_json_ld(parsed))
    return [text for text in texts if text]


def _walk_json_ld(node: object) -> list[str]:
    texts: list[str] = []
    if isinstance(node, dict):
        article_body = node.get("articleBody")
        if isinstance(article_body, str):
            normalized = normalize_whitespace(article_body)
            if len(normalized) >= 80:
                texts.append(normalized)
        for value in node.values():
            texts.extend(_walk_json_ld(value))
    elif isinstance(node, list):
        for value in node:
            texts.extend(_walk_json_ld(value))
    return texts


def _iter_density_candidates(soup: BeautifulSoup) -> Iterable[Tag]:
    selectors = ["article", "main", "section", "div"]
    seen: set[int] = set()
    for selector in selectors:
        for node in soup.select(selector):
            if isinstance(node, Tag) and id(node) not in seen:
                seen.add(id(node))
                yield node


def _extract_text_density_fallback(soup: BeautifulSoup) -> str:
    best_text = ""
    best_score = -1
    for container in _iter_density_candidates(soup):
        paragraphs = [
            normalize_whitespace(node.get_text(" ", strip=True))
            for node in container.find_all(["p", "li"])
        ]
        paragraphs = [text for text in paragraphs if len(text) >= 40]
        if len(paragraphs) < 2:
            continue
        text = normalize_whitespace(" ".join(paragraphs))
        if len(text) < 120:
            continue
        link_count = len(container.find_all("a"))
        score = len(text) - (link_count * 40)
        if score > best_score:
            best_score = score
            best_text = text
    return best_text


def extract_main_article_text(html: str) -> str:
    if not normalize_whitespace(html):
        return ""

    soup = BeautifulSoup(html, "html.parser")
    _remove_noise(soup)

    candidate_texts: list[str] = []
    candidate_texts.extend(_extract_json_ld_texts(soup))
    for selector in CONTENT_SELECTORS:
        for container in soup.select(selector):
            if not isinstance(container, Tag):
                continue
            text = _container_text(container)
            if text:
                candidate_texts.append(text)

    if not candidate_texts:
        density_fallback = _extract_text_density_fallback(soup)
        if density_fallback:
            candidate_texts.append(density_fallback)

    if not candidate_texts:
        body = soup.body or soup
        fallback = _container_text(body)
        if fallback:
            candidate_texts.append(fallback)

    if not candidate_texts:
        text = normalize_whitespace(soup.get_text(" ", strip=True))
        return text

    return max(candidate_texts, key=len)
