from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from taxdash import (  # noqa: E402
    load_and_process_data,
    load_and_process_sped_fiscal,
    load_and_process_ecd,
)


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "arquivos_teste"


def test_contribuicoes_loader_returns_unique_ids():
    sample = FIXTURE_DIR / "PISCOFINS_20250101.txt"
    with sample.open("rb") as fh:
        df = load_and_process_data(fh)

    assert not df.empty
    assert df["id"].is_unique
    assert {"periodo", "cnpj", "id_pai"}.issubset(df.columns)


def test_sped_fiscal_loader_handles_single_file():
    sample = FIXTURE_DIR / "84501873000178-063002558-20250101-SPED-EFD.txt"
    with sample.open("rb") as fh:
        df = load_and_process_sped_fiscal([fh])

    assert not df.empty
    assert df["id"].is_unique
    assert {"cnpj_estab", "periodo"}.issubset(df.columns)


def test_ecd_loader_accepts_multiple_files():
    sample1 = FIXTURE_DIR / "84501873000178-13200278755-20230101-20231231-G-CA7AE9E0F6F799E378293DDCF7B7CEC4385248FF-1-SPED-ECD.txt"
    sample2 = FIXTURE_DIR / "ecd2020.txt"
    with sample1.open("rb") as fh1, sample2.open("rb") as fh2:
        df = load_and_process_ecd([fh1, fh2])

    assert not df.empty
    assert df["id"].is_unique
