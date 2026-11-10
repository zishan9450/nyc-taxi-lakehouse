import pytest

from lakehouse.ingest import months_between, sha256_file, validate_month
from lakehouse.quality import QualityError, check_not_empty, check_reject_rate


@pytest.mark.parametrize("good", ["2024-01", "2025-12"])
def test_validate_month_accepts(good):
    assert validate_month(good) == good


@pytest.mark.parametrize("bad", ["2024-13", "2024-1", "24-01", "2024/01", "", "2024-00"])
def test_validate_month_rejects(bad):
    with pytest.raises(ValueError):
        validate_month(bad)


def test_months_between_crosses_year_boundary():
    assert months_between("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]
    assert months_between("2024-05", "2024-05") == ["2024-05"]
    assert months_between("2024-06", "2024-05") == []


def test_sha256_file(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"abc")
    assert sha256_file(p) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_reject_rate_gate():
    assert check_reject_rate(1, 100, 0.05) == 0.01
    with pytest.raises(QualityError):
        check_reject_rate(10, 100, 0.05)
    with pytest.raises(QualityError):
        check_reject_rate(0, 0, 0.05)


def test_not_empty_gate():
    check_not_empty(1, "x")
    with pytest.raises(QualityError):
        check_not_empty(0, "x")
