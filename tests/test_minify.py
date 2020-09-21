# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.
#

from pathlib import Path

import pytest

import cc_net
import cc_net.minify as minify
from cc_net import process_wet_file
from cc_net.minify import (
    HASH_SIZE,
    decode_hashes,
    encode_as_hashes,
    encode_hashes,
    get_hashes,
)


def test_encode_decode():
    sentences = ["Hello world !", "Is everyone happy in here ?"]
    hashes = get_hashes(sentences)
    assert all([len(h) == HASH_SIZE for h in hashes])
    hashes_int = [minify._b2i(h) for h in hashes]
    encoded = encode_hashes(hashes)
    decoded = decode_hashes(encoded)
    assert all([len(d) == HASH_SIZE for d in decoded])

    decoded_int = [minify._b2i(d) for d in decoded]

    assert hashes_int == decoded_int
    assert hashes == decoded


def test_minify():
    doc = {
        "raw_content": "Hello world !\nIs everyone happy in here ?",
        "language": "en",
        "perplexity": 120.0,
    }
    expected = {"hashes": "fApSnZA0cQg=", "language": "en", "perplexity": 120.0}

    minifier = minify.Minifier()
    assert expected == minifier(doc)


@pytest.fixture
def http_from_disk(monkeypatch):
    def read_sample_file(url: str, n_retry: int = 3) -> bytes:
        expected_url = process_wet_file.WET_URL_ROOT + "/crawl-data/sample.warc.txt"
        assert expected_url == url
        file = Path(__file__).parent / "data" / "sample.warc.txt"
        return file.read_bytes()

    monkeypatch.setattr(cc_net.jsonql, "request_get_content", read_sample_file)


def test_unminify(http_from_disk):
    # same quotes minus the "Education: ..." one
    quotes = """Don't part with your illusions. When they are gone you may still exist, but you have ceased to live.
Facts are stubborn things, but statistics are more pliable.
Fiction is obliged to stick to possibilities. Truth isn't."""

    doc = {
        "url": "http://sample_english.com",
        "date_download": "2019-03-18T00:00:00Z",
        "digest": "sha1:XQZHW7QWIG54HVAV3KPRW6MK5ILDNCER",
        "source_domain": "sample_english.com",
        "title": "Famous Mark Twain Quotes",
        "raw_content": quotes,
        "cc_segment": "crawl-data/sample.warc.txt",
        "nlines": 3,
        "length": len(quotes),
        "original_nlines": 4,
        "original_length": 353,
        "language": "en",
        "language_score": 0.99,
        "perplexity": 151.5,
        "bucket": "head",
    }

    # make a copy of doc since minifier operates in place
    mini = minify.Minifier()(dict(**doc))
    assert mini != doc
    unminifier = minify.Unminifier()
    assert doc == unminifier(mini)


def test_unminify_hit_mem_cache(http_from_disk):
    mini_docs = [
        {
            "url": "http://sample_english.com",
            "digest": "sha1:XQZHW7QWIG54HVAV3KPRW6MK5ILDNCER",
            "cc_segment": "crawl-data/sample.warc.txt",
            "hashes": encode_as_hashes(
                ["Facts are stubborn things, but statistics are more pliable."]
            ),
        },
        {
            "url": "http://sample_chinese.com",
            "digest": "sha1:Y4E6URVYGIAFNVRTPZ5S3J64RTZTP6HJ",
            "cc_segment": "crawl-data/sample.warc.txt",
            "hashes": encode_as_hashes(["事實是固執的東西，但統計數字卻比較柔和。"]),
        },
    ]
    unminifier = minify.Unminifier()
    unminifier.look_for(mini_docs)
    contents = [d["raw_content"] for d in unminifier.map(mini_docs)]
    assert unminifier.retrieved_segments == 1

    assert [
        "Facts are stubborn things, but statistics are more pliable.",
        "事實是固執的東西，但統計數字卻比較柔和。",
    ] == contents
