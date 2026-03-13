from scripts.update_controlled_access_state import build_payload


def test_build_payload_includes_priority_controlled_sources_and_guides():
    payload = build_payload()

    guide_keys = {guide["key"] for guide in payload["process_guides"]}
    source_keys = {source["key"] for source in payload["sources"]}

    assert "ega" in guide_keys
    assert "qphi" in guide_keys
    assert "emirati_population_variome" in source_keys
    assert "qphi_qatari_25k" in source_keys
    assert any("EGA" in step or "QPHI" in step for source in payload["sources"] for step in source["access_steps"])
