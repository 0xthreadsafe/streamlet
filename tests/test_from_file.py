"""``Stream.from_file``: reading files as lazy streams.

Closing guarantees are covered separately in ``test_file_closing.py``.
"""

from pathlib import Path

import pytest

from streamlet import FileStream, Stream

LOG = "INFO boot\nERROR disk full\nINFO ready\nERROR net down\n"


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    path = tmp_path / "app.log"
    path.write_text(LOG, encoding="utf-8")
    return path


def test_yields_every_line(log_file: Path) -> None:
    assert Stream.from_file(log_file).to_list() == LOG.splitlines(keepends=True)


def test_lines_keep_their_newlines(log_file: Path) -> None:
    """Matches open(); use .map(str.rstrip) to strip them."""
    assert Stream.from_file(log_file).first() == "INFO boot\n"
    assert Stream.from_file(log_file).map(str.rstrip).first() == "INFO boot"


def test_returns_a_file_stream(log_file: Path) -> None:
    assert isinstance(Stream.from_file(log_file), FileStream)


def test_accepts_a_string_path(log_file: Path) -> None:
    assert Stream.from_file(str(log_file)).count() == 4


def test_supports_the_full_stream_api(log_file: Path) -> None:
    with Stream.from_file(log_file) as lines:
        errors = lines.map(str.rstrip).filter(lambda line: line.startswith("ERROR")).to_list()
    assert errors == ["ERROR disk full", "ERROR net down"]


def test_group_lines_by_level(log_file: Path) -> None:
    with Stream.from_file(log_file) as lines:
        groups = lines.map(str.rstrip).group_by(lambda line: line.split(" ", 1)[0])
    assert sorted(groups) == ["ERROR", "INFO"]
    assert groups["ERROR"] == ["ERROR disk full", "ERROR net down"]


def test_reads_lazily_one_line_at_a_time(tmp_path: Path) -> None:
    """A huge file must not be materialised to read a couple of lines."""
    path = tmp_path / "big.txt"
    path.write_text("".join(f"line {n}\n" for n in range(100_000)), encoding="utf-8")
    with Stream.from_file(path) as lines:
        assert lines.map(str.rstrip).take(2).to_list() == ["line 0", "line 1"]


def test_empty_file_gives_an_empty_stream(tmp_path: Path) -> None:
    path = tmp_path / "empty.txt"
    path.write_text("", encoding="utf-8")
    assert Stream.from_file(path).to_list() == []


def test_file_without_a_trailing_newline(tmp_path: Path) -> None:
    path = tmp_path / "no_newline.txt"
    path.write_text("a\nb", encoding="utf-8")
    assert Stream.from_file(path).to_list() == ["a\n", "b"]


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Stream.from_file(tmp_path / "nope.txt")


def test_encoding_is_honoured(tmp_path: Path) -> None:
    path = tmp_path / "latin.txt"
    path.write_bytes("café\n".encode("latin-1"))
    assert Stream.from_file(path, encoding="latin-1").first() == "café\n"


def test_encoding_errors_can_be_ignored(tmp_path: Path) -> None:
    path = tmp_path / "bad.txt"
    path.write_bytes(b"caf\xe9\n")
    with pytest.raises(UnicodeDecodeError):
        Stream.from_file(path).to_list()
    assert Stream.from_file(path, errors="ignore").first() == "caf\n"


def test_is_single_use_like_any_stream(log_file: Path) -> None:
    from streamlet import StreamConsumedError

    stream = Stream.from_file(log_file)
    stream.to_list()
    with pytest.raises(StreamConsumedError):
        stream.to_list()


def test_pipes_like_any_stream(log_file: Path) -> None:
    def errors_only(stream: Stream[str]) -> Stream[str]:
        return stream.filter(lambda line: "ERROR" in line)

    with Stream.from_file(log_file) as lines:
        assert (lines | errors_only | (lambda s: s.count())) == 2
