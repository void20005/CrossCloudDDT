import pathlib


def test_conftest_contains_only_ascii():
    """Fail if `conftest.py` contains non-ASCII characters (emoji or other high codepoints).

    This helps catch accidental emoji/Unicode in test setup which can break Windows consoles.
    """
    p = pathlib.Path("conftest.py")
    text = p.read_text(encoding="utf-8")
    non_ascii = sorted({c for c in text if ord(c) > 127})
    assert not non_ascii, f"Found non-ASCII characters in conftest.py: {non_ascii}"
